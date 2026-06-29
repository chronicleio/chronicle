use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

pub fn copy_range(
    src: &File,
    src_offset: u64,
    dst: &File,
    dst_offset: u64,
    length: u64,
) -> io::Result<u64> {
    if length == 0 {
        return Ok(0);
    }

    #[cfg(target_os = "linux")]
    {
        return copy_range_linux(src, src_offset, dst, dst_offset, length);
    }

    #[cfg(not(target_os = "linux"))]
    {
        copy_range_fallback(src, src_offset, dst, dst_offset, length)
    }
}

#[cfg(target_os = "linux")]
fn copy_range_linux(
    src: &File,
    src_offset: u64,
    dst: &File,
    dst_offset: u64,
    length: u64,
) -> io::Result<u64> {
    use std::os::unix::io::AsRawFd;

    let src_fd = src.as_raw_fd();
    let dst_fd = dst.as_raw_fd();
    let mut off_in = src_offset as i64;
    let mut off_out = dst_offset as i64;
    let mut remaining = length as usize;

    while remaining > 0 {
        let ret = unsafe {
            libc::copy_file_range(src_fd, &mut off_in, dst_fd, &mut off_out, remaining, 0)
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOSYS) || err.raw_os_error() == Some(libc::EXDEV) {
                return copy_range_fallback(src, src_offset, dst, dst_offset, length);
            }
            return Err(err);
        }

        if ret == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "copy_file_range returned 0",
            ));
        }

        remaining -= ret as usize;
    }

    Ok(length)
}

fn copy_range_fallback(
    src: &File,
    src_offset: u64,
    dst: &File,
    dst_offset: u64,
    length: u64,
) -> io::Result<u64> {
    let mut buf = vec![0u8; length as usize];
    src.read_exact_at(&mut buf, src_offset)?;
    dst.write_all_at(&buf, dst_offset)?;
    Ok(length)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_copy_range_basic() {
        let dir = tempfile::tempdir().unwrap();

        let src_path = dir.path().join("src.dat");
        let dst_path = dir.path().join("dst.dat");

        let data = b"hello world, this is a test of copy_range";
        {
            let mut f = File::create(&src_path).unwrap();
            f.write_all(data).unwrap();
        }

        {
            let mut f = File::create(&dst_path).unwrap();
            f.write_all(b"\0\0\0\0\0").unwrap();
        }

        let src = File::open(&src_path).unwrap();
        let dst = File::options().write(true).open(&dst_path).unwrap();

        let copied = copy_range(&src, 6, &dst, 5, 5).unwrap();
        assert_eq!(copied, 5);

        let mut result = vec![0u8; 10];
        let dst_read = File::open(&dst_path).unwrap();
        dst_read.read_exact_at(&mut result, 0).unwrap();
        assert_eq!(&result[5..10], b"world");
    }

    #[test]
    fn test_copy_range_full_file() {
        let dir = tempfile::tempdir().unwrap();

        let src_path = dir.path().join("src2.dat");
        let dst_path = dir.path().join("dst2.dat");

        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        {
            let mut f = File::create(&src_path).unwrap();
            f.write_all(&data).unwrap();
        }

        File::create(&dst_path).unwrap();

        let src = File::open(&src_path).unwrap();
        let dst = File::options().write(true).open(&dst_path).unwrap();

        copy_range(&src, 0, &dst, 0, 256).unwrap();

        let mut result = vec![0u8; 256];
        let dst_read = File::open(&dst_path).unwrap();
        dst_read.read_exact_at(&mut result, 0).unwrap();
        assert_eq!(result, data);
    }
}
