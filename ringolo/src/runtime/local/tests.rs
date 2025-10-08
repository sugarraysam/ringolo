use crate::future::opcodes::NopBuilder;
use crate::runtime::Builder;
use crate::sqe::Sqe;
use crate::test_utils::*;
use anyhow::Result;

#[test]
fn test_local_worker_single_nop() -> Result<()> {
    let builder = Builder::new_local().try_build()?;
    builder.block_on(async {
        let fut = NopBuilder::new().build()?;
        let res = fut.await;

        assert!(res.is_ok());
        let (_, res) = res.unwrap();

        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
        Ok(())
    })
}

#[test]
fn test_local_worker_batch_of_nops() -> Result<()> {
    let builder = Builder::new_local().try_build()?;
    builder.block_on(async {
        let batch = Sqe::new(build_batch(10)?);
        let res = batch.await;

        assert!(res.is_ok());
        for (_, res) in res.unwrap() {
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), 0);
        }

        Ok(())
    })
}

#[test]
fn test_local_worker_chain_write_fsync_read_tempfile() -> Result<()> {
    let builder = Builder::new_local().try_build()?;
    builder.block_on(async {
        let (chain, data_out, mut data_in, _tmp) = build_chain_write_fsync_read_a_tempfile()?;
        let chain = Sqe::new(chain);
        let res = chain.await;

        assert!(res.is_ok());

        let expected: Vec<i32> = vec![data_out.len() as i32, 0, data_out.len() as i32];
        expected
            .into_iter()
            .zip(res.unwrap())
            .for_each(|(expected, (_entry, got))| {
                assert!(got.is_ok());
                assert_eq!(expected, got.unwrap())
            });

        // vec is unaware that data was written as it was through a *mut T
        unsafe {
            data_in.set_len(data_out.len());
        }

        assert_eq!(data_in, data_out);

        Ok(())
    })
}
