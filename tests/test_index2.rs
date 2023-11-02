use blockfile2::Length;
use std::mem::{align_of, size_of};
use std::path::PathBuf;
use std::str::FromStr;
use textindex::error::AppError;
use textindex::index2::word_map::{RawWordMap, RawWordMapList};
use textindex::index2::words::{RawWord, RawWordList};
use textindex::index2::{FileId, Words};

#[test]
fn test_sizes() {
    const BLOCK_SIZE: usize = 4096;
    println!("RawWordmapList {}", size_of::<RawWordMapList>());
    println!("RawWordmapList {}", align_of::<RawWordMapList>());
    println!("RawWordmapList::LEN {}", RawWordMapList::LEN);
    println!("RawWordMap {}", size_of::<RawWordMap>());
    println!("RawWordMap {}", align_of::<RawWordMap>());
    println!("RawWordList {}", size_of::<RawWordList>());
    println!("RawWordList {}", align_of::<RawWordList>());
    println!("RawWordList::LEN {}", RawWordList::LEN);
    println!("RawWord {}", size_of::<RawWord>());
    println!("RawWord {}", align_of::<RawWord>());

    assert_eq!(BLOCK_SIZE, size_of::<RawWordMapList>());
    assert_eq!(0, BLOCK_SIZE % size_of::<RawWordMap>());
    assert_eq!(BLOCK_SIZE, size_of::<RawWordList>());
    assert_eq!(0, BLOCK_SIZE % size_of::<RawWord>());
}

#[test]
fn test_numeric() {
    let word = "09feb97:";
    if let Some(c) = word.chars().next() {
        // numeric data ignored
        if c.is_numeric() {
            return;
        }
    }
    panic!();
}

#[test]
fn test_init() -> Result<(), AppError> {
    let path = PathBuf::from_str("tmp/init.idx")?;

    let mut w = Words::create(&path)?;
    dbg!(&w);
    w.store_to_db()?;
    dbg!(&w);
    w.write()?;
    let w = Words::read(&path)?;
    dbg!(&w);
    Ok(())
}

#[test]
fn test_files() -> Result<(), AppError> {
    let path = PathBuf::from_str("tmp/files.idx")?;

    let mut w = Words::create(&path)?;
    let _fid = w.add_file("file0".into());
    w.write()?;
    let w = Words::read(&path)?;

    assert!(w.files().contains_key(&FileId(1)));

    Ok(())
}

#[test]
fn test_files2() -> Result<(), AppError> {
    let path = PathBuf::from_str("tmp/files2.idx")?;

    let mut w = Words::create(&path)?;
    let _fid = w.add_file("file0".into());
    let _fid = w.add_file("file1".into());
    let _fid = w.add_file("file2".into());
    let _fid = w.add_file("file3".into());

    w.store_to_db()?;
    println!("{:#?}", w);
    w.write()?;

    let w = Words::read(&path)?;

    let mut it = w.files().iter();
    let f0 = it.next().unwrap();
    assert_eq!(*f0.0, 1);
    assert_eq!(f0.1.name, "file0");
    assert_eq!(f0.1.block_nr, 3);
    assert_eq!(f0.1.block_idx, 0);

    let f1 = it.next().unwrap();
    assert_eq!(*f1.0, 2);
    assert_eq!(f1.1.name, "file1");
    assert_eq!(f1.1.block_nr, 3);
    assert_eq!(f1.1.block_idx, 10);

    let f2 = it.next().unwrap();
    assert_eq!(*f2.0, 3);
    assert_eq!(f2.1.name, "file2");
    assert_eq!(f2.1.block_nr, 3);
    assert_eq!(f2.1.block_idx, 20);

    let f3 = it.next().unwrap();
    assert_eq!(*f3.0, 4);
    assert_eq!(f3.1.name, "file3");
    assert_eq!(f3.1.block_nr, 3);
    assert_eq!(f3.1.block_idx, 30);

    Ok(())
}

#[test]
fn test_word() -> Result<(), AppError> {
    let path = PathBuf::from_str("tmp/word.idx")?;

    let mut w = Words::create(&path)?;
    let fid = w.add_file("file0".into());
    w.add_word("alpha", fid)?;
    w.write()?;

    let mut w = Words::read(&path)?;

    assert!(w.words().get("alpha").is_some());
    if let Some(word) = w.words().get("alpha").cloned() {
        assert_eq!(word.file_map_block_nr, 0);
        assert_eq!(word.file_map_idx, 0);
        assert_eq!(word.first_file_id, 1);
        assert_eq!(word.id, 1);
        let mut it = w.iter_word_files(word);
        assert_eq!(it.next().unwrap()?, 1);
        assert!(it.next().is_none());
    }

    Ok(())
}

#[test]
fn test_word2() -> Result<(), AppError> {
    let path = PathBuf::from_str("tmp/word2.idx")?;

    let mut w = Words::create(&path)?;
    let fid = w.add_file("file0".into());
    w.add_word("alpha", fid)?;
    w.add_word("beta", fid)?;
    w.add_word("gamma", fid)?;
    w.add_word("delta", fid)?;
    w.add_word("epsilon", fid)?;
    w.write()?;

    let w = Words::read(&path)?;

    assert!(w.words().get("alpha").is_some());
    assert!(w.words().get("beta").is_some());
    assert!(w.words().get("gamma").is_some());
    assert!(w.words().get("delta").is_some());
    assert!(w.words().get("epsilon").is_some());

    Ok(())
}

#[test]
fn test_word3() -> Result<(), AppError> {
    let path = PathBuf::from_str("tmp/word3.idx")?;

    let mut w = Words::create(&path)?;
    let fid = w.add_file("file0".into());
    w.add_word("alpha", fid)?;
    w.add_word("beta", fid)?;
    w.add_word("gamma", fid)?;
    w.add_word("delta", fid)?;
    w.add_word("epsilon", fid)?;
    let fid = w.add_file("file1".into());
    w.add_word("alpha", fid)?;
    w.add_word("beta", fid)?;
    w.add_word("gamma", fid)?;
    w.write()?;

    let mut w = Words::read(&path)?;
    // println!("{:#1?}", w);

    assert!(w.words().get("alpha").is_some());
    assert!(w.words().get("beta").is_some());
    assert!(w.words().get("gamma").is_some());
    assert!(w.words().get("delta").is_some());
    assert!(w.words().get("epsilon").is_some());

    let wdata = w.words().get("alpha").cloned().unwrap();
    assert_eq!(wdata.file_map_block_nr, 3);
    assert_eq!(wdata.file_map_idx, 0);
    assert_eq!(wdata.first_file_id, 0);
    {
        let mut it = w.iter_word_files(wdata);
        assert_eq!(it.next().unwrap()?, 1);
        assert_eq!(it.next().unwrap()?, 2);
        assert!(it.next().is_none());
    }

    let wdata = w.words().get("beta").cloned().unwrap();
    assert_eq!(wdata.file_map_block_nr, 3);
    assert_eq!(wdata.file_map_idx, 1);
    let mut it = w.iter_word_files(wdata);
    assert_eq!(it.next().unwrap()?, 1);
    assert_eq!(it.next().unwrap()?, 2);
    assert!(it.next().is_none());

    Ok(())
}

#[test]
fn test_word4() -> Result<(), AppError> {
    let path = PathBuf::from_str("tmp/word4.idx")?;

    let mut w = Words::create(&path)?;
    let fid = w.add_file("file0".into());
    w.add_word("alpha", fid)?;
    w.add_word("beta", fid)?;
    w.add_word("gamma", fid)?;
    w.add_word("delta", fid)?;
    w.add_word("epsilon", fid)?;

    let _wdata = w.words().get("gamma").cloned().unwrap();

    let fid = w.add_file("file1".into());
    w.add_word("alpha", fid)?;
    w.add_word("beta", fid)?;
    w.add_word("gamma", fid)?;

    let _wdata = w.words().get("gamma").cloned().unwrap();

    for i in 0..14 {
        let fid = w.add_file(format!("file-x{}", i));
        w.add_word("gamma", fid)?;

        let _wdata = w.words().get("gamma").cloned().unwrap();
    }
    // dbg!(&w);
    w.write()?;

    let mut w = Words::read(&path)?;

    let wdata = w.words().get("gamma").cloned().unwrap();

    let fid = w
        .iter_word_files(wdata)
        .map(|v| v.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        fid.as_slice(),
        &[13, 14, 15, 16, 7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6,]
    );

    println!("{:2?}", w);

    Ok(())
}
