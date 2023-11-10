use crate::error::AppError;
use crate::index2::tmp_index::TmpWords;
use crate::index2::Words;
use crate::proc3::indexer::{index_html, index_txt};
use crate::proc3::threads::{Msg, Work, WorkerState};
use rustyline::ExternalPrinter;
use std::borrow::Cow;
use std::cmp::min;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};
use wildmatch::WildMatch;

pub mod indexer;
pub mod stop_words;
pub mod threads;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FileFilter {
    Ignore,
    Inspect,
    Dubious,
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

pub fn load_file(filter: FileFilter, absolute: &Path) -> Result<(FileFilter, String), AppError> {
    let mut buf = Vec::new();
    File::open(&absolute)?.read_to_end(&mut buf)?;
    let str = String::from_utf8_lossy(buf.as_slice());
    let filter = content_filter(filter, str.as_ref());

    Ok((filter, str.into()))
}

pub fn indexing(filter: FileFilter, relative: &str, txt: &str) -> (FileFilter, TmpWords) {
    let mut words = TmpWords::new(relative);

    match filter {
        FileFilter::Text => {
            index_txt(&mut words, txt);
        }
        FileFilter::Html => {
            index_html(&mut words, txt);
        }
        FileFilter::Ignore => {}
        FileFilter::Inspect => {}
        FileFilter::Dubious => {}
    }

    (filter, words)
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
        ".stored",
        ".tmp_stored",
        "thumbs.db",
        "index.html",
        "jan.html",
        "feb.html",
        "mar.html",
        "apr.html",
        "may.html",
        "jun.html",
        "jul.html",
        "aug.html",
        "sep.html",
        "oct.html",
        "nov.html",
        "dec.html",
        "ctur_seven2^4.html",
        "my_hot_little_sister.html",
        "kindergarten_manager.html",
    ];
    const PREFIX_IGNORE: &[&str] = &["week"];

    if EXT_IGNORE.contains(&ext.as_str())
        || NAME_IGNORE.contains(&name.as_str())
        || PREFIX_IGNORE.iter().any(|v| name.starts_with(*v))
    {
        FileFilter::Ignore
    } else {
        FileFilter::Inspect
    }
}

pub fn content_filter(filter: FileFilter, txt: &str) -> FileFilter {
    if filter == FileFilter::Ignore {
        return filter;
    }

    const HTML_RECOGNIZE: &[&str] = &[
        "<!--ADULTSONLY",
        "<--",
        "<head",
        "<HTML",
        "<html",
        "<?xml",
        "<!DOCTYPE",
        "<!doctype",
        "_<!DOCTYPE",
    ];

    if HTML_RECOGNIZE
        .iter()
        .any(|v| txt.trim_start().starts_with(*v))
    {
        FileFilter::Html
    } else {
        let txt_part = &txt.as_bytes()[0..min(256, txt.len())];
        for c in txt_part.iter().copied() {
            #[allow(unused_comparisons)]
            if c >= 0 && c <= 8 || c >= 11 && c <= 12 || c >= 14 && c <= 31 {
                return FileFilter::Dubious;
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

        let (_filter, txt) = load_file(FileFilter::Inspect, &path)?;
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
