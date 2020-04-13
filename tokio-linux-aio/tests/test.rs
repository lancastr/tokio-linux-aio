use std::{fs, mem};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, select_biased, StreamExt};
use futures::channel::oneshot;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use tempfile::NamedTempFile;
use tokio::time::delay_for;

use tokio_linux_aio::{AioContext, LockedBuf, AioFile};

const FILE_SIZE: usize = 1024 * 512;
const BUF_CAPACITY: usize = 8192;

fn fill_temp_file(file: &mut fs::File) {
    let mut data = [0u8; FILE_SIZE];

    for index in 0..data.len() {
        data[index] = index as u8;
    }

    file.write(&data).unwrap();
    file.sync_all().unwrap();
}


#[tokio::test]
async fn read_block_mt() {
    let mut temp_file = NamedTempFile::new().unwrap();
    fill_temp_file(temp_file.as_file_mut());
    let (_, path) = temp_file.keep().unwrap();

    {
        let file = AioFile::open(&path).unwrap();

        let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();

        let aio = AioContext::new(10).unwrap();

        file.read(&aio, 0, buffer.as_mut()).await.unwrap();

        assert!(validate_block(buffer.as_ref()));

        assert_eq!(10, aio.available_slots());
    }

    fs::remove_file(&path).unwrap()
}

#[tokio::test]
async fn write_block_mt() {
    let mut temp_file = NamedTempFile::new().unwrap();
    fill_temp_file(temp_file.as_file_mut());
    let (_, path) = temp_file.keep().unwrap();

    {
        let file = AioFile::open(&path).unwrap();

        let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();
        fill_pattern(65u8, buffer.as_mut());

        let aio = AioContext::new(10).unwrap();

        file.write(&aio, 16384, buffer.as_ref()).await.unwrap();
    }

    let mut file = File::open(&path).unwrap();

    file.seek(SeekFrom::Start(16384)).unwrap();

    let mut read_buffer: [u8; BUF_CAPACITY] = [0u8; BUF_CAPACITY];
    file.read(&mut read_buffer).unwrap();

    assert!(validate_pattern(65u8, &read_buffer));

    fs::remove_file(&path).unwrap();
}

#[tokio::test]
async fn write_block_sync_mt() {
    let mut temp_file = NamedTempFile::new().unwrap();
    fill_temp_file(temp_file.as_file_mut());
    let (_, path) = temp_file.keep().unwrap();

    {
        let file = AioFile::open(&path).unwrap();

        let aio = AioContext::new(2).unwrap();

        {
            let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();
            fill_pattern(65u8, buffer.as_mut());
            file.write(&aio, 16384, buffer.as_ref()).await.unwrap();
        }

        {
            let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();
            fill_pattern(66u8, buffer.as_mut());
            file.write(&aio, 32768, buffer.as_ref()).await.unwrap();
        }

        {
            let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();
            fill_pattern(67u8, buffer.as_mut());
            file.write(&aio, 49152, buffer.as_ref()).await.unwrap();
        }
    }

    let mut file = File::open(&path).unwrap();

    let mut read_buffer: [u8; BUF_CAPACITY] = [0u8; BUF_CAPACITY];

    file.seek(SeekFrom::Start(16384)).unwrap();
    file.read(&mut read_buffer).unwrap();
    assert!(validate_pattern(65u8, &read_buffer));

    file.seek(SeekFrom::Start(32768)).unwrap();
    file.read(&mut read_buffer).unwrap();
    assert!(validate_pattern(66u8, &read_buffer));

    file.seek(SeekFrom::Start(49152)).unwrap();
    file.read(&mut read_buffer).unwrap();
    assert!(validate_pattern(67u8, &read_buffer));

    fs::remove_file(&path).unwrap();
}

#[tokio::test]
async fn invalid_offset() {
    let mut temp_file = NamedTempFile::new().unwrap();
    fill_temp_file(temp_file.as_file_mut());
    let (_, path) = temp_file.keep().unwrap();

    let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();

    {
        let file = AioFile::open(&path).unwrap();

        let aio = AioContext::new(10).unwrap();
        let res = file.read(&aio, 1000000, buffer.as_mut()).await;

        assert!(res.is_err());
    }

    fs::remove_file(&path).unwrap();
}

#[tokio::test]
async fn future_cancellation() {
    let mut temp_file = NamedTempFile::new().unwrap();
    fill_temp_file(temp_file.as_file_mut());
    let (_, path) = temp_file.keep().unwrap();

    let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();

    {
        let file = AioFile::open(&path).unwrap();

        let num_slots = 10;

        let aio = AioContext::new(num_slots).unwrap();
        let mut read = Box::pin(file.read(&aio, 0, buffer.as_mut()).fuse());

        let (_, immediate) = oneshot::channel::<()>();

        let mut immediate = immediate.fuse();

        select_biased! {
                _ = read => {
                    assert!(false);
                },
                _ = immediate => {},
            }

        mem::drop(read);

        while aio.available_slots() != num_slots {
            delay_for(Duration::from_millis(50)).await;
        }
    }

    fs::remove_file(&path).unwrap();
}

#[tokio::test]
async fn read_many_blocks_mt() {
    let mut temp_file = NamedTempFile::new().unwrap();
    fill_temp_file(temp_file.as_file_mut());
    let (_, path) = temp_file.keep().unwrap();

    let file = Arc::new(AioFile::open(&path).unwrap());

    let num_slots = 7;
    let aio = AioContext::new(num_slots).unwrap();

    // 50 waves of requests just going above the limit

    // Waves start here
    for _wave in 0u64..50 {
        let f = FuturesUnordered::new();
        let aio = aio.clone();
        let file = file.clone();

        // Each wave makes 100 I/O requests
        for index in 0u64..100 {
            let file = file.clone();
            let aio = aio.clone();

            f.push(async move {
                // let aio = aio.clone();

                let offset = (index * BUF_CAPACITY as u64) % FILE_SIZE as u64;
                let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();

                file.read(&aio, offset, buffer.as_mut()).await.unwrap();

                assert!(validate_block(buffer.as_ref()));
            });
        }

        let _ = f.collect::<Vec<_>>().await;

        // all slots have been returned
        assert_eq!(num_slots, aio.available_slots());
    }

    fs::remove_file(&path).unwrap();
}

// A test with a mixed read/write workload
#[tokio::test]
async fn mixed_read_write() {
    let mut temp_file = NamedTempFile::new().unwrap();
    fill_temp_file(temp_file.as_file_mut());
    let (_, path) = temp_file.keep().unwrap();

    let file = Arc::new(AioFile::open(&path).unwrap());

    let aio = AioContext::new(7).unwrap();

    let seq1 = {
        let file = file.clone();
        let aio = aio.clone();

        async move {
            let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();

            file.read(&aio, 8192, buffer.as_mut()).await.unwrap();
            assert!(validate_block(buffer.as_ref()));

            fill_pattern(0u8, buffer.as_mut());
            file.write(&aio, 8192, buffer.as_mut()).await.unwrap();

            file.read(&aio, 0, buffer.as_mut()).await.unwrap();
            assert!(validate_block(buffer.as_ref()));

            fill_pattern(1u8, buffer.as_mut());
            file.write(&aio, 0, buffer.as_ref()).await.unwrap();

            file.read(&aio, 8192, buffer.as_mut()).await.unwrap();
            assert!(validate_pattern(0u8, buffer.as_ref()));

            file.read(&aio, 0, buffer.as_mut()).await.unwrap();
            assert!(validate_pattern(1u8, buffer.as_ref()));
        }
    };

    let seq2 = {
        let file = file.clone();
        let aio = aio.clone();

        async move {
            let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();

            file.read(&aio, 16384, buffer.as_mut()).await.unwrap();
            assert!(validate_block(buffer.as_ref()));

            fill_pattern(2u8, buffer.as_mut());
            file.write(&aio, 16384, buffer.as_mut()).await.unwrap();

            file.read(&aio, 24576, buffer.as_mut()).await.unwrap();
            assert!(validate_block(buffer.as_ref()));

            fill_pattern(3, buffer.as_mut());
            file.write(&aio, 24576, buffer.as_ref()).await.unwrap();

            file.read(&aio, 16384, buffer.as_mut()).await.unwrap();
            assert!(validate_pattern(2, buffer.as_ref()));

            file.read(&aio, 24576, buffer.as_mut()).await.unwrap();
            assert!(validate_pattern(3u8, buffer.as_ref()));
        }
    };

    let seq3 = {
        async move {
            let mut buffer = LockedBuf::with_capacity(BUF_CAPACITY).unwrap();

            file.read(&aio, 40960, buffer.as_mut()).await.unwrap();
            assert!(validate_block(buffer.as_ref()));

            fill_pattern(5u8, buffer.as_mut());
            file.write(&aio, 40960, buffer.as_mut()).await.unwrap();

            file.read(&aio, 32768, buffer.as_mut()).await.unwrap();
            assert!(validate_block(buffer.as_ref()));

            fill_pattern(6, buffer.as_mut());
            file.write(&aio, 32768, buffer.as_ref()).await.unwrap();

            file.read(&aio, 40960, buffer.as_mut()).await.unwrap();
            assert!(validate_pattern(5, buffer.as_ref()));

            file.read(&aio, 32768, buffer.as_mut()).await.unwrap();
            assert!(validate_pattern(6, buffer.as_ref()));
        }
    };

    join_all(vec![
        tokio::spawn(seq1),
        tokio::spawn(seq2),
        tokio::spawn(seq3),
    ])
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
}

fn fill_pattern(key: u8, buffer: &mut [u8]) {
    assert_eq!(buffer.len() % 2, 0);

    for index in 0..buffer.len() / 2 {
        buffer[index * 2] = key;
        buffer[index * 2 + 1] = index as u8;
    }
}

fn validate_pattern(key: u8, buffer: &[u8]) -> bool {
    assert_eq!(buffer.len() % 2, 0);

    for index in 0..buffer.len() / 2 {
        if (buffer[index * 2] != key) || (buffer[index * 2 + 1] != (index as u8)) {
            return false;
        }
    }

    return true;
}

fn validate_block(data: &[u8]) -> bool {
    for index in 0..data.len() {
        if data[index] != index as u8 {
            return false;
        }
    }

    true
}