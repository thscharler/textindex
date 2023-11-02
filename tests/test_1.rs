use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::io::Read;
use std::path::Path;
use textindex::index2::Words;
use textindex::proc3::{content_filter, name_filter, FileFilter};
use textindex::tmp_index::{index_html, index_txt, TmpWords};
use walkdir::WalkDir;

#[test]
fn test_filter() -> Result<(), io::Error> {
    let sample = "samples/index";
    let path = Path::new(sample);

    let mut buf = Vec::new();

    for f in WalkDir::new(path).into_iter().flatten() {
        if !f.metadata()?.is_file() {
            println!("{:?}", f.path().file_name().unwrap());
            continue;
        }

        let filter = name_filter(&f.path());

        buf.clear();
        File::open(f.path())?.read_to_end(&mut buf)?;
        let text = String::from_utf8_lossy(buf.as_slice());

        let filter2 = content_filter(filter, text.as_ref());

        if filter2 == FileFilter::Text {
            println!();
            println!(
                "{:?} len={} filter1={:?} filter2={:?} txt={}",
                f.path().file_name().unwrap(),
                f.metadata()?.len(),
                filter,
                filter2,
                text.chars()
                    .take(30)
                    .map(|v| if v == '\n' { '_' } else { v })
                    .collect::<String>()
            );
        }
    }

    Ok(())
}

#[test]
fn test_index() -> Result<(), io::Error> {
    let sample = "samples/index";
    let path = Path::new(sample);

    let mut buf = Vec::new();

    let mut word_stat: BTreeMap<String, usize> = BTreeMap::new();
    let mut cnt_file: usize = 0;
    for f in WalkDir::new(path).into_iter().flatten() {
        if !f.metadata()?.is_file() {
            println!("-- DIR {:?}", f.path().file_name().unwrap());
            continue;
        }
        println!("{:?}", f.path().file_name().unwrap());

        cnt_file += 1;

        let filter = name_filter(&f.path());
        buf.clear();
        File::open(f.path())?.read_to_end(&mut buf)?;
        let text = String::from_utf8_lossy(buf.as_slice());
        let filter = content_filter(filter, text.as_ref());

        let mut words = TmpWords::new(".");
        match filter {
            FileFilter::Ignore => {
                println!("ignore");
            }
            FileFilter::Inspect => {
                println!("inspect");
            }
            FileFilter::Text => {
                index_txt(&mut words, text.as_ref());
            }
            FileFilter::Html => {
                index_html(&mut words, text.as_ref());
            }
        }

        for word in words.words {
            word_stat.entry(word).and_modify(|v| *v += 1).or_insert(1);
        }
    }

    let mut stat: BTreeMap<usize, usize> = BTreeMap::new();
    for (_, n) in &word_stat {
        stat.entry(*n).and_modify(|v| *v += 1).or_insert(1);
    }
    let sum: usize = stat.values().sum();
    let mut partial = 0usize;
    for (n, cnt) in stat {
        partial += cnt;
        println!("{}: {} | {}%", n, cnt, partial * 100 / sum);
    }

    for (word, n) in word_stat {
        if n <= (cnt_file as f64 * 0.1) as usize {
            println!("{}: {}", word, n);
        }
    }

    Ok(())
}

#[test]
fn test_merge() -> Result<(), io::Error> {
    let sample = "samples/index";
    let path = Path::new(sample);

    let mut words = Words::create(Path::new("tmp/merge.db")).unwrap();

    let mut buf = Vec::new();

    let mut cnt_file: usize = 0;
    for f in WalkDir::new(path).into_iter().flatten() {
        if !f.metadata()?.is_file() {
            println!("-- DIR {:?}", f.path().file_name().unwrap());
            continue;
        }
        println!("{:?}", f.path().file_name().unwrap());

        cnt_file += 1;

        let filter = name_filter(&f.path());
        buf.clear();
        File::open(f.path())?.read_to_end(&mut buf)?;
        let text = String::from_utf8_lossy(buf.as_slice());
        let filter = content_filter(filter, text.as_ref());

        let mut tmp_words = TmpWords::new(f.path().to_string_lossy());
        match filter {
            FileFilter::Ignore => {
                println!("ignore");
            }
            FileFilter::Inspect => {
                println!("inspect");
            }
            FileFilter::Text => {
                index_txt(&mut tmp_words, text.as_ref());
            }
            FileFilter::Html => {
                index_html(&mut tmp_words, text.as_ref());
            }
        }

        words.append(tmp_words).unwrap();
    }

    words.write().unwrap();

    println!("{:2?}", words);

    Ok(())
}
