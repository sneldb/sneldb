use std::cell::RefCell;

thread_local! {
    static SCRATCH: RefCell<Vec<u8>> = RefCell::new(Vec::new());
}

pub fn decompress_into_pool<E, F>(out_len: usize, mut with_decompress: F) -> Result<Vec<u8>, E>
where
    F: FnMut(&mut [u8]) -> Result<(), E>,
{
    SCRATCH.with(|slot| {
        // Move the buffer out first to avoid overlapping RefCell borrows
        let mut buf = {
            let mut cell = slot.borrow_mut();
            std::mem::take(&mut *cell)
        };
        if buf.capacity() < out_len {
            buf.reserve(out_len - buf.capacity());
        }
        buf.resize(out_len, 0);
        with_decompress(&mut buf[..])?;
        // Slot already holds an empty Vec (from take); return the filled buffer
        Ok(buf)
    })
}
