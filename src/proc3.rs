use crate::error::AppError;
use crate::index2::tmp_index::TmpWords;
use crate::index2::Words;
use crate::proc3::indexer::{index_html2, index_txt2};
use crate::proc3::threads::{Msg, Work, WorkerState};
use rustyline::ExternalPrinter;
use std::borrow::Cow;
use std::cmp::min;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};
#[cfg(feature = "allocator")]
use tracking_allocator::AllocationGroupToken;
use wildmatch::WildMatch;

pub mod html_parse;
mod html_parse2;
pub mod indexer;
mod named_char;
pub mod stop_words;
pub mod threads;
pub mod txt_parse;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FileFilter {
    Ignore,
    Inspect,
    Text,
    Html,
}

#[derive(Default)]
pub struct Found {
    pub terms: Vec<String>,

    pub files: Vec<String>,

    pub lines_idx: usize,
    pub lines: Vec<(String, Vec<String>)>,
}

pub struct Data {
    pub words: Mutex<Words>,
    pub found: Mutex<Found>,
    pub log: File,
}

impl Data {
    pub fn write(&'static self) -> Result<(), AppError> {
        if let Ok(mut wrl) = self.words.lock() {
            wrl.write()?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn read(path: &Path) -> Result<&'static Data, AppError> {
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open("log.txt")?;

        let words = Words::read(path)?;

        let data: &'static Data = Box::leak(Box::new(Data {
            words: Mutex::new(words),
            found: Default::default(),
            log,
        }));

        Ok(data)
    }
}

pub fn shut_down(work: &Work) {
    println!("sending shutdown!");
    if let Err(e) = work.send.send(Msg::Quit) {
        if let Ok(mut print) = work.printer.lock() {
            let _ = print.print(format!("shutdown {:?}", e));
        }
    }

    if let Ok(mut print) = work.printer.lock() {
        let _ = print.print("wait on shutdown".into());
    }

    sleep(Duration::from_millis(100));

    for w in work.workers.iter() {
        if !w.handle.is_finished() {
            continue;
        }
    }
}

pub fn load_file(filter: FileFilter, absolute: &Path) -> Result<(FileFilter, Vec<u8>), AppError> {
    let mut buf = Vec::new();
    File::open(&absolute)?.read_to_end(&mut buf)?;

    if filter == FileFilter::Inspect {
        let mut buf = [0u8; 256];

        let mut file = File::open(&absolute)?;
        let n = file.read(&mut buf)?;
        match content_filter(&buf[..n]) {
            FileFilter::Ignore => Ok((FileFilter::Ignore, Vec::new())),
            f => {
                file.seek(SeekFrom::Start(0))?;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)?;
                Ok((f, buf))
            }
        }
    } else {
        let mut buf = Vec::new();
        File::open(&absolute)?.read_to_end(&mut buf)?;
        Ok((filter, buf))
    }
}

pub fn indexing(
    log: &mut File,
    #[cfg(feature = "allocator")] tok_txt: &mut AllocationGroupToken,
    #[cfg(feature = "allocator")] tok_html: &mut AllocationGroupToken,
    #[cfg(feature = "allocator")] tok_tmpwords: &mut AllocationGroupToken,
    filter: FileFilter,
    relative: &str,
    txt: &Vec<u8>,
) -> Result<(FileFilter, TmpWords), io::Error> {
    let mut words = TmpWords::new(relative);
    let txt = String::from_utf8_lossy(txt.as_ref());

    match filter {
        FileFilter::Text => {
            index_txt2(
                log,
                #[cfg(feature = "allocator")]
                tok_txt,
                #[cfg(feature = "allocator")]
                tok_tmpwords,
                relative,
                &mut words,
                txt.as_ref(),
            )?;
        }
        FileFilter::Html => {
            index_html2(
                log,
                #[cfg(feature = "allocator")]
                tok_txt,
                #[cfg(feature = "allocator")]
                tok_html,
                #[cfg(feature = "allocator")]
                tok_tmpwords,
                relative,
                &mut words,
                txt.as_ref(),
            )?;
        }
        FileFilter::Ignore => {}
        FileFilter::Inspect => {}
    }

    Ok((filter, words))
}

pub fn merge_words(
    data: &'static Data,
    state: &Arc<Mutex<WorkerState>>,
    words_buffer: TmpWords,
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> Result<(), AppError> {
    let do_auto_save = {
        state.lock().unwrap().state = 100;
        let mut write = data.words.lock()?;
        state.lock().unwrap().state = 101;

        timing(printer, "merge", 100, || write.append(words_buffer))?;
        state.lock().unwrap().state = 102;

        let auto_save = write.should_auto_save();
        if auto_save {
            let last = write.save_time();
            print_(printer, format!("loop-time {:?}", last.elapsed()));
            write.set_save_time();
        }
        auto_save
    };

    if do_auto_save {
        state.lock().unwrap().state = 200;
        timing(printer, "autosave", 1, || auto_save(printer, data))?;
    }

    Ok(())
}

pub fn name_filter(path: &Path) -> FileFilter {
    let ext = path
        .extension()
        .map(|v| v.to_string_lossy())
        .unwrap_or(Cow::Borrowed(""))
        .to_lowercase();
    let name = path
        .file_name()
        .map(|v| v.to_string_lossy())
        .unwrap_or(Cow::Borrowed(""))
        .to_lowercase();

    const EXT_IGNORE: &[&str] = &[
        "jpg", "pdf", "gif", "css", "png", "doc", "rtf", "js", "ico", "woff", "zip", "jpeg", "odt",
        "docx", "lit", "xml", "epub", "mobi", "exe", "mp3", "azw3", "bmp", "bak", "ccs", "css",
        "dwt", "eot", "img", "pdb", "prc", "psc", "swf", "svg", "wmf", "wpd", "wav", "mso", "mid",
        "thmx", "zblorb", "rm", "ttf", "woff2", "eot", "emz", "mht",
    ];
    const NAME_IGNORE: &[&str] = &[
        ".message.ftp.txt",
        "history.txt",
        "stored.idx",
        "log.txt",
        "thumbs.db",
        // "jan.html",
        // "feb.html",
        // "mar.html",
        // "apr.html",
        // "may.html",
        // "jun.html",
        // "jul.html",
        // "aug.html",
        // "sep.html",
        // "oct.html",
        // "nov.html",
        // "dec.html",
        // "week1.html",
        // "week2.html",
        // "week3.html",
        // "week4.html",
        // "week5.html",
        // "week6.html",
        // "week7.html",
        // "week8.html",
        // "week9.html",
        // "week10.html",
        // "week11.html",
        // "week12.html",
        // "week13.html",
        // "week14.html",
        // "week15.html",
        // "week16.html",
        // "week17.html",
        // "week18.html",
        // "week19.html",
        // "week20.html",
        // "week21.html",
        // "week22.html",
        // "week23.html",
        // "week24.html",
        // "week25.html",
        // "week26.html",
        // "week27.html",
        // "week28.html",
        // "week29.html",
        // "week30.html",
        // "week31.html",
        // "week32.html",
        // "week33.html",
        // "week34.html",
        // "week35.html",
        // "week36.html",
        // "week37.html",
        // "week38.html",
        // "week39.html",
        // "week40.html",
        // "week41.html",
        // "week42.html",
        // "week43.html",
        // "week44.html",
        // "week45.html",
        // "week46.html",
        // "week47.html",
        // "week48.html",
        // "week49.html",
        // "week50.html",
        // "week51.html",
        // "week52.html",
        // "week53.html",
    ];

    if EXT_IGNORE.contains(&ext.as_str()) || NAME_IGNORE.contains(&name.as_str()) {
        FileFilter::Ignore
    } else {
        FileFilter::Inspect
    }
}

pub fn content_filter(txt: &[u8]) -> FileFilter {
    const HTML_RECOGNIZE: &[&[u8]] = &[
        b"<!--ADULTSONLY",
        b"<--",
        b"<head",
        b"<HTML",
        b"<html",
        b"<?xml",
        b"<!DOCTYPE",
        b"<!doctype",
        b"_<!DOCTYPE",
    ];

    // omit starting whitespace
    let mut start_idx = 0;
    for i in 0..txt.len() {
        if txt[i] != b' ' && txt[i] != b'\t' && txt[i] != b'\n' && txt[i] != b'\r' {
            start_idx = i;
            break;
        }
    }
    // dont scan everything
    let txt_part = &txt[start_idx..min(start_idx + txt.len(), txt.len())];

    if HTML_RECOGNIZE.iter().any(|v| txt_part.starts_with(*v)) {
        FileFilter::Html
    } else {
        for c in txt_part.iter().copied() {
            #[allow(unused_comparisons)]
            if c >= 0 && c <= 8 || c >= 11 && c <= 12 || c >= 14 && c <= 31 {
                return FileFilter::Ignore;
            }
        }
        FileFilter::Text
    }
}

pub fn auto_save(
    _printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    data: &'static Data,
) -> Result<(), AppError> {
    data.write()?;
    Ok(())
}

fn delete_file(
    _printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    data: &'static Data,
    file: String,
) -> Result<(), AppError> {
    let mut write = data.words.lock()?;
    write.remove_file(file);

    Ok(())
}

fn print_<S: Into<String>>(printer: &Arc<Mutex<dyn ExternalPrinter + Send>>, msg: S) {
    if let Ok(mut print) = printer.lock() {
        let _ = print.print(msg.into());
    }
}

fn print_err_(
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    mut log: File,
    task: &str,
    res: Result<(), AppError>,
) {
    if let Err(err) = res {
        let _ = writeln!(log, "{} {:#?}", task, err);
        if let Ok(mut print) = printer.lock() {
            let _ = print.print(format!("{} {:?}", task, err));
        }
    }
}

fn timing<S: AsRef<str>, R>(
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    name: S,
    threshold: u64,
    fun: impl FnOnce() -> R,
) -> R {
    let now = Instant::now();

    let result = fun();

    let timing = now.elapsed();
    if timing > Duration::from_millis(threshold) {
        print_(printer, format!("{} {:?}", name.as_ref(), now.elapsed()));
    }

    result
}

// Search the result files and return matching text-lines.
pub fn find_matched_lines(
    terms: &[String],
    files: &Vec<String>,
) -> Result<Vec<(String, Vec<String>)>, AppError> {
    let terms: Vec<_> = terms.iter().map(|v| WildMatch::new(v)).collect();

    // get the text-lines that contain any of the search-terms.
    let mut result = Vec::new();
    for file in files {
        let path = PathBuf::from(".");
        let path = path.join(&file);

        let mut txt = Vec::new();
        File::open(&path)?.read_to_end(&mut txt)?;

        let txt = String::from_utf8_lossy(txt.as_ref());

        let mut text_lines = Vec::new();
        for line in txt.split('\n') {
            let mut print_line = false;

            'line: for word in line.split(' ') {
                for term in &terms {
                    if term.matches(word) {
                        print_line = true;
                        break 'line;
                    }
                }
            }

            if print_line {
                text_lines.push(line.to_string());
            }
        }

        result.push((file.clone(), text_lines));
    }

    Ok(result)
}
