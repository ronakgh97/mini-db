use bytes::Bytes;
use mini_db::wal::{CRC_LEN, HEADER_LEN, Wal};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn tmp_path(name: &str) -> PathBuf {
    let id = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "mini-db-{}-{}-{}-wal-test.log",
        std::process::id(),
        name,
        id
    ))
}

async fn cleanup(path: &PathBuf) {
    let _ = tokio::fs::remove_file(path).await;
}

fn record_len(klen: usize, vlen: usize) -> u64 {
    (HEADER_LEN + klen + vlen + CRC_LEN) as u64
}

#[tokio::test]
async fn empty_init_returns_empty_map() -> anyhow::Result<()> {
    let path = tmp_path("empty");
    cleanup(&path).await;

    let (wal, map) = Wal::init(path.clone()).await?;
    assert!(map.is_empty());
    assert_eq!(wal.next_offset(), 0);
    assert_eq!(wal.entry_count(), 0);

    cleanup(&path).await;
    Ok(())
}

#[tokio::test]
async fn set_entries_survive_recovery() -> anyhow::Result<()> {
    let path = tmp_path("set-recover");
    cleanup(&path).await;

    let (mut wal, _) = Wal::init(path.clone()).await?;
    let off0 = wal
        .append(OP_SET, &Bytes::from("k1"), &Bytes::from("v1"))
        .await?;
    let off1 = wal
        .append(OP_SET, &Bytes::from("k2"), &Bytes::from("v2"))
        .await?;
    assert_eq!(off0, 0);
    assert_eq!(off1, record_len(2, 2));
    wal.fsync().await?;
    assert_eq!(wal.entry_count(), 2);
    drop(wal);

    let (wal2, map) = Wal::init(path.clone()).await?;
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&Bytes::from("k1")).unwrap(), &Bytes::from("v1"));
    assert_eq!(map.get(&Bytes::from("k2")).unwrap(), &Bytes::from("v2"));
    assert_eq!(wal2.entry_count(), 2);
    assert_eq!(wal2.next_offset(), record_len(2, 2) * 2);

    cleanup(&path).await;
    Ok(())
}

#[tokio::test]
async fn delete_removes_key_on_recovery() -> anyhow::Result<()> {
    let path = tmp_path("delete");
    cleanup(&path).await;

    let (mut wal, _) = Wal::init(path.clone()).await?;
    wal.append(OP_SET, &Bytes::from("k1"), &Bytes::from("v1"))
        .await?;
    wal.append(OP_SET, &Bytes::from("k2"), &Bytes::from("v2"))
        .await?;
    wal.append(OP_DELETE, &Bytes::from("k1"), &Bytes::from_static(b""))
        .await?;
    wal.fsync().await?;
    drop(wal);

    let (wal2, map) = Wal::init(path.clone()).await?;
    assert_eq!(map.len(), 1);
    assert!(!map.contains_key(&Bytes::from("k1")));
    assert_eq!(map.get(&Bytes::from("k2")).unwrap(), &Bytes::from("v2"));
    // 2 SETs + 1 DELETE tombstone all count as entries.
    assert_eq!(wal2.entry_count(), 3);

    cleanup(&path).await;
    Ok(())
}

#[tokio::test]
async fn append_rejects_structurally_invalid() -> anyhow::Result<()> {
    let path = tmp_path("reject");
    cleanup(&path).await;

    let (mut wal, _) = Wal::init(path.clone()).await?;

    // GET(0) and unknown ops must never be logged.
    assert!(
        wal.append(0, &Bytes::from("k"), &Bytes::from("v"))
            .await
            .is_err()
    );
    assert!(
        wal.append(99, &Bytes::from("k"), &Bytes::from("v"))
            .await
            .is_err()
    );
    // Empty key would be unrecoverable (replay breaks on klen==0).
    assert!(
        wal.append(OP_SET, &Bytes::from_static(b""), &Bytes::from("v"))
            .await
            .is_err()
    );
    // DELETE must carry empty value.
    assert!(
        wal.append(OP_DELETE, &Bytes::from("k"), &Bytes::from("nonempty"))
            .await
            .is_err()
    );

    // Failed appends must not advance state.
    assert_eq!(wal.next_offset(), 0);
    assert_eq!(wal.entry_count(), 0);

    cleanup(&path).await;
    Ok(())
}

#[tokio::test]
async fn torn_tail_is_truncated_on_init() -> anyhow::Result<()> {
    let path = tmp_path("torn");
    cleanup(&path).await;

    let (mut wal, _) = Wal::init(path.clone()).await?;
    wal.append(OP_SET, &Bytes::from("k1"), &Bytes::from("v1"))
        .await?;
    wal.append(OP_SET, &Bytes::from("k2"), &Bytes::from("v2"))
        .await?;
    wal.fsync().await?;
    let good_len = tokio::fs::metadata(&path).await?.len();
    drop(wal);

    // Simulate crash mid-write: 3 garbage bytes at EOF (shorter than header+CRC).
    {
        use tokio::io::AsyncWriteExt;
        let mut f = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&path)
            .await?;
        f.write_all(&[1, 2, 3]).await?;
        f.flush().await?;
    }
    assert!(tokio::fs::metadata(&path).await?.len() > good_len);

    let (wal2, map) = Wal::init(path.clone()).await?;
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&Bytes::from("k1")).unwrap(), &Bytes::from("v1"));
    // Truncated back to last valid entry — next append overwrites cleanly.
    assert_eq!(wal2.next_offset(), good_len);
    assert_eq!(tokio::fs::metadata(&path).await?.len(), good_len);

    cleanup(&path).await;
    Ok(())
}

#[tokio::test]
async fn crc_mismatch_discards_tail_suffix() -> anyhow::Result<()> {
    let path = tmp_path("crc");
    cleanup(&path).await;

    let (mut wal, _) = Wal::init(path.clone()).await?;
    wal.append(OP_SET, &Bytes::from("k1"), &Bytes::from("v1"))
        .await?;
    wal.append(OP_SET, &Bytes::from("k2"), &Bytes::from("v2"))
        .await?;
    wal.fsync().await?;
    drop(wal);

    let good_len = record_len(2, 2); // only k1 survives after we corrupt k2
    let mut bytes = tokio::fs::read(&path).await?;
    // Flip a byte inside k2's value (second record) to break its CRC.
    let k2_value_offset = (record_len(2, 2) as usize) + HEADER_LEN + 2;
    bytes[k2_value_offset] ^= 0xFF;
    tokio::fs::write(&path, &bytes).await?;

    let (wal2, map) = Wal::init(path.clone()).await?;
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&Bytes::from("k1")).unwrap(), &Bytes::from("v1"));
    assert!(!map.contains_key(&Bytes::from("k2")));
    assert_eq!(wal2.next_offset(), good_len);
    assert_eq!(tokio::fs::metadata(&path).await?.len(), good_len);

    cleanup(&path).await;
    Ok(())
}

#[tokio::test]
async fn unknown_op_tail_is_truncated_not_panicking() -> anyhow::Result<()> {
    let path = tmp_path("unknown-op");
    cleanup(&path).await;

    let (mut wal, _) = Wal::init(path.clone()).await?;
    wal.append(OP_SET, &Bytes::from("k1"), &Bytes::from("v1"))
        .await?;
    wal.fsync().await?;
    let good_len = tokio::fs::metadata(&path).await?.len();
    drop(wal);

    // Well-formed-length tail with bogus op byte must be treated as corrupt tail.
    {
        use tokio::io::AsyncWriteExt;
        let mut f = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&path)
            .await?;
        // op=99, klen=1, vlen=1, key, value, fake crc — must not panic recovery.
        f.write_all(&[99, 1, 0, 0, 0, 1, 0, 0, 0, b'x', b'y', 0, 0, 0, 0])
            .await?;
        f.flush().await?;
    }

    let (wal2, map) = Wal::init(path.clone()).await?;
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&Bytes::from("k1")).unwrap(), &Bytes::from("v1"));
    assert_eq!(wal2.next_offset(), good_len);
    assert_eq!(tokio::fs::metadata(&path).await?.len(), good_len);

    cleanup(&path).await;
    Ok(())
}

#[tokio::test]
async fn offsets_are_monotonic_record_starts() -> anyhow::Result<()> {
    let path = tmp_path("offsets");
    cleanup(&path).await;

    let (mut wal, _) = Wal::init(path.clone()).await?;
    let o0 = wal
        .append(OP_SET, &Bytes::from("a"), &Bytes::from("1111"))
        .await?;
    let o1 = wal
        .append(OP_SET, &Bytes::from("bb"), &Bytes::from("22"))
        .await?;
    let o2 = wal
        .append(OP_DELETE, &Bytes::from("a"), &Bytes::from_static(b""))
        .await?;

    assert_eq!(o0, 0);
    assert_eq!(o1, record_len(1, 4));
    assert_eq!(o2, record_len(1, 4) + record_len(2, 2));
    assert_eq!(wal.next_offset(), o2 + record_len(1, 0));
    assert_eq!(wal.entry_count(), 3);

    cleanup(&path).await;
    Ok(())
}
