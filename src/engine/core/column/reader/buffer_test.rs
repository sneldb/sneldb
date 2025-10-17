use crate::engine::core::column::reader::buffer::decompress_into_pool;

#[test]
fn decompress_into_pool_fills_provided_len() {
    let out = decompress_into_pool(16, |dst| -> Result<(), ()> {
        for (i, b) in dst.iter_mut().enumerate() {
            *b = (i as u8) ^ 0xAA;
        }
        Ok(())
    })
    .expect("pool ok");
    assert_eq!(out.len(), 16);
    assert_eq!(out[0], 0xAA);
    assert_eq!(out[1], 0xAB);
}

#[test]
fn decompress_into_pool_reusable_buffer() {
    // First call allocates
    let _ = decompress_into_pool(8, |dst| -> Result<(), ()> {
        for b in dst.iter_mut() {
            *b = 1;
        }
        Ok(())
    })
    .unwrap();
    // Second call with larger size should still succeed
    let out = decompress_into_pool(32, |dst| -> Result<(), ()> {
        for (i, b) in dst.iter_mut().enumerate() {
            *b = i as u8;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(out.len(), 32);
    assert_eq!(out[31], 31);
}
