use crate::error::AppError;
use crate::index::{index_html, index_txt, Words};
use crossbeam::channel::{bounded, Receiver, Sender, TryRecvError};
use rustyline::ExternalPrinter;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::iter::Flatten;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{sleep, JoinHandle};
use std::time::{Duration, Instant};
use std::{fs, thread};
use walkdir::WalkDir;

#[derive(Debug)]
pub enum Msg {
    Quit,
    WalkTree(PathBuf),
    WalkFinished(PathBuf),
    Load(u32, FileFilter, PathBuf, String),
    Index(u32, FileFilter, PathBuf, String, String),
    MergeWords(u32, Words),
    DeleteFile(String),
    Debug,
    AutoSave,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FileFilter {
    Ignore,
    Inspect,
    Text,
    Html,
}

pub struct Data {
    pub words: RwLock<Words>,
    pub modified: Mutex<bool>,
}

impl Data {
    pub fn write(&'static self, path: &Path) -> Result<(), AppError> {
        if let Ok(rdl) = self.words.try_read() {
            rdl.write(path)
        } else {
            println!("fail lock autosave?!");
            Ok(())
        }
    }

    pub fn read(path: &Path) -> Result<&'static Data, AppError> {
        let words = Words::read(path)?;

        let data: &'static Data = Box::leak(Box::new(Data {
            words: RwLock::new(words),
            modified: Mutex::new(false),
        }));

        Ok(data)
    }
}

pub struct Work {
    pub send: Sender<Msg>,
    pub recv_send: [(Receiver<Msg>, Sender<Msg>); 4],
    pub recv: Receiver<Msg>,
    pub handles: [JoinHandle<()>; 8],

    pub printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
}

pub fn init_work<P: ExternalPrinter + Send + Sync + 'static>(
    printer: P,
    data: &'static Data,
) -> Work {
    let printer = Arc::new(Mutex::new(printer));

    let (s0, r1) = bounded::<Msg>(10);
    let (s1, r2) = bounded::<Msg>(10);
    let (s2, r3) = bounded::<Msg>(10);
    let (s3, r4) = bounded::<Msg>(10);
    let (s4, r5) = bounded::<Msg>(10);

    let h1 = spawn_walking(r1.clone(), s1.clone(), data, printer.clone());
    let h2 = spawn_loading(r2.clone(), s2.clone(), data, printer.clone());
    let h3_1 = spawn_indexing(r3.clone(), s3.clone(), data, printer.clone());
    let h3_2 = spawn_indexing(r3.clone(), s3.clone(), data, printer.clone());
    let h3_3 = spawn_indexing(r3.clone(), s3.clone(), data, printer.clone());
    let h3_4 = spawn_indexing(r3.clone(), s3.clone(), data, printer.clone());
    let h4 = spawn_merge_words(r4.clone(), s4.clone(), data, printer.clone());
    let h5 = spawn_terminal(r5.clone(), data, printer.clone());

    Work {
        send: s0,
        recv_send: [(r1, s1), (r2, s2), (r3, s3), (r4, s4)],
        recv: r5,
        handles: [h1, h2, h3_1, h3_2, h3_3, h3_4, h4, h5],
        printer,
    }
}

pub fn shut_down(work: &Work) {
    println!("sending shutdown!");
    if let Err(e) = work.send.send(Msg::Quit) {
        if let Ok(mut print) = work.printer.lock() {
            let _ = print.print(format!("shutdown {:?}", e));
        }
    }

    loop {
        if let Ok(mut print) = work.printer.lock() {
            let _ = print.print("wait on shutdown".into());
        }

        sleep(Duration::from_millis(100));

        for h in work.handles.iter() {
            if !h.is_finished() {
                continue;
            }
        }

        break;
    }
}

fn spawn_walking(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    struct WalkingProc {
        path: PathBuf,
        tree_iter: Flatten<walkdir::IntoIter>,
        count: u32,
        files_seen: HashSet<String>,
    }

    fn walk_proc(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        data: &'static Data,
        printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> Result<(), AppError> {
        // This is a bit more complicated, as we need to keep up the message flow
        // while traversing the directory tree. We interweave each step of the tree iteration
        // and message processing.

        let mut proc = None;

        loop {
            if proc.is_none() {
                match recv.recv()? {
                    Msg::Quit => {
                        send.send(Msg::Quit)?;
                        break;
                    }
                    Msg::Debug => {
                        print_(printer, "walk_tree empty");
                        send.send(Msg::Debug)?;
                    }
                    Msg::WalkTree(path) => {
                        proc = Some(WalkingProc {
                            path: path.clone(),
                            tree_iter: WalkDir::new(path).into_iter().flatten(),
                            count: 0,
                            files_seen: {
                                let read = data.words.read()?;
                                read.files.iter().cloned().collect::<HashSet<_>>()
                            },
                        });
                    }
                    msg => {
                        send.send(msg)?;
                    }
                }
            } else {
                match recv.try_recv() {
                    Ok(Msg::Quit) => {
                        send.send(Msg::Quit)?;
                        break;
                    }
                    Ok(Msg::Debug) => {
                        let Some(proc) = &mut proc else {
                            unreachable!()
                        };
                        print_(printer, format!("walk_tree {}", proc.count));
                        send.send(Msg::Debug)?;
                    }
                    Ok(Msg::WalkTree(_)) => {
                        if let Ok(mut print) = printer.lock() {
                            let _ = print.print(format!(
                                "new tree walk ignored, still working on the last one."
                            ));
                        }
                    }
                    Ok(msg) => {
                        send.send(msg)?;
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {
                        break;
                    }
                }

                let Some(sproc) = &mut proc else {
                    unreachable!()
                };

                if let Some(entry) = sproc.tree_iter.next() {
                    let meta = entry.metadata()?;
                    if meta.is_file() {
                        let absolute = entry.path();
                        let relative = entry
                            .path()
                            .strip_prefix(&sproc.path)
                            .unwrap_or(absolute)
                            .to_string_lossy()
                            .to_string();

                        let filter = name_filter(&absolute);

                        if filter == FileFilter::Ignore {
                            // print_(&printer, format!("ignore {:?}", relative));
                            continue;
                        }

                        if !sproc.files_seen.contains(&relative) {
                            sproc.count += 1;
                            send.send(Msg::Load(sproc.count, filter, absolute.into(), relative))?;
                        } else {
                            // print_(&printer, format!("seen {:?}", relative));
                        }
                    }
                } else {
                    send.send(Msg::WalkFinished(sproc.path.clone()))?;
                    proc = None;
                }
            }
        }

        Ok(())
    }

    thread::spawn(move || {
        print_err_(&printer, "walker", walk_proc(recv, send, data, &printer));
    })
}

fn spawn_loading(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    fn load_proc(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        _data: &'static Data,
        printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> Result<(), AppError> {
        let mut last_count = 0;

        loop {
            match recv.recv()? {
                Msg::Quit => {
                    send.send(Msg::Quit)?;
                    break;
                }
                Msg::Debug => {
                    print_(printer, format!("loading {}", last_count));
                    send.send(Msg::Debug)?;
                }
                Msg::Load(count, filter, absolute, relative) => {
                    last_count = count;
                    loading(printer, count, filter, absolute, relative, &send)?;
                }
                msg => send.send(msg)?,
            }
        }
        Ok(())
    }

    thread::spawn(move || {
        print_err_(&printer, "loading", load_proc(recv, send, data, &printer));
    })
}

fn spawn_indexing(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    fn index_proc(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        _data: &'static Data,
        printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> Result<(), AppError> {
        let mut last_count = 0;

        loop {
            match recv.recv()? {
                Msg::Quit => {
                    send.send(Msg::Quit)?;
                    break;
                }
                Msg::Debug => {
                    print_(printer, format!("indexing {}", last_count));
                    send.send(Msg::Debug)?;
                }
                Msg::Index(count, filter, absolute, relative, txt) => {
                    last_count = count;
                    indexing(printer, count, filter, absolute, relative, &txt, &send)?;
                }
                msg => send.send(msg)?,
            }
        }
        Ok(())
    }

    thread::spawn(move || {
        print_err_(&printer, "indexing", index_proc(recv, send, data, &printer));
    })
}

fn spawn_merge_words(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    fn merge_words_proc(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        data: &'static Data,
        printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> Result<(), AppError> {
        let mut last_count = 0;

        loop {
            match recv.recv()? {
                Msg::Quit => {
                    send.send(Msg::Quit)?;
                    break;
                }
                Msg::Debug => {
                    print_(printer, format!("merge words {}", last_count));
                    send.send(Msg::Debug)?;
                }
                Msg::MergeWords(count, words) => {
                    last_count = count;
                    print_err_(printer, "merge_words", merge_words(printer, words, data));

                    // ...
                    if count % 1000 == 0 {
                        print_(printer, format!("merged {}", count));
                    }
                }
                msg => send.send(msg)?,
            }
        }
        Ok(())
    }

    thread::spawn(move || {
        print_err_(
            &printer,
            "merge_words",
            merge_words_proc(recv, send, data, &printer),
        )
    })
}

fn spawn_terminal(
    recv: Receiver<Msg>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    fn terminal_proc(
        recv: &Receiver<Msg>,
        data: &'static Data,
        printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> Result<(), AppError> {
        loop {
            match recv.recv()? {
                Msg::Quit => {
                    break;
                }
                Msg::Debug => {
                    print_(printer, "terminal");
                }
                Msg::AutoSave => {
                    print_err_(&printer, "auto_save", auto_save(printer, data));
                }
                Msg::DeleteFile(file) => {
                    print_err_(&printer, "delete_file", delete_file(printer, data, file));
                }
                Msg::WalkFinished(file) => {
                    print_(&printer, format!("*** {:?} finished ***", file));
                }
                msg => {
                    print_(&printer, format!("invalid terminal message {:?}", msg));
                }
            }
        }
        Ok(())
    }

    thread::spawn(move || {
        print_err_(&printer, "terminal", terminal_proc(&recv, data, &printer));
    })
}

fn name_filter(path: &Path) -> FileFilter {
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

    if name == ".message.ftp.txt"
        || name == "history.txt"
        || name == ".stored"
        || name == ".tmp_stored"
    {
        FileFilter::Ignore
    } else if name.starts_with("index.html") {
        FileFilter::Ignore
    } else if name.starts_with("week") {
        FileFilter::Ignore
    } else if [
        "apr.html", "aug.html", "dec.html", "feb.html", "jan.html", "jul.html", "jun.html",
        "may.html", "mar.html", "nov.html", "oct.html", "sep.html",
    ]
    .binary_search_by(|v| v.cmp(&name.as_str()))
    .is_ok()
    {
        FileFilter::Ignore
    } else if ext == "jpg" || ext == "pdf" || ext == "gif" || ext == "css" || ext == "png" {
        FileFilter::Ignore
    } else if ext == "doc" {
        FileFilter::Ignore
    } else if ext == "rtf" {
        FileFilter::Ignore
    } else if ext == "html" || ext == "htm" {
        FileFilter::Html
    } else if ext == "txt" {
        FileFilter::Text
    } else {
        FileFilter::Inspect
    }
}

fn loading(
    _printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    count: u32,
    filter: FileFilter,
    absolute: PathBuf,
    relative: String,
    send: &Sender<Msg>,
) -> Result<(), AppError> {
    let mut buf = Vec::new();
    File::open(&absolute)?.read_to_end(&mut buf)?;
    let str = String::from_utf8_lossy(buf.as_slice());

    let filter = content_filter(filter, str.as_ref());

    if filter != FileFilter::Ignore {
        send.send(Msg::Index(count, filter, absolute, relative, str.into()))?;
    }
    Ok(())
}

fn content_filter(filter: FileFilter, txt: &str) -> FileFilter {
    if filter != FileFilter::Inspect {
        return filter;
    }

    if txt.starts_with("<?xml")
        || txt.starts_with("<!DOCTYPE")
        || txt.starts_with("<html")
        || txt.starts_with("<!--")
    {
        FileFilter::Html
    } else {
        FileFilter::Ignore
    }
}

fn indexing(
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    count: u32,
    filter: FileFilter,
    _absolute: PathBuf,
    relative: String,
    txt: &String,
    send: &Sender<Msg>,
) -> Result<(), AppError> {
    let mut words = Words::new();

    match filter {
        FileFilter::Text => {
            let file_idx = words.add_file(relative.clone());
            timing(printer, format!("indexing {:?}", relative), 100, || {
                index_txt(&mut words, file_idx, &txt)
            });
        }
        FileFilter::Html => {
            let file_idx = words.add_file(relative.clone());
            timing(printer, format!("indexing {:?}", relative), 100, || {
                index_html(&mut words, file_idx, &txt)
            });
        }
        FileFilter::Ignore => {}
        FileFilter::Inspect => {}
    }

    send.send(Msg::MergeWords(count, words))?;
    Ok(())
}

fn merge_words(
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    words: Words,
    data: &'static Data,
) -> Result<(), AppError> {
    let mut do_auto_save = false;

    {
        let mut write = data.words.write()?;
        timing(printer, "merge", 100, || write.append(words));

        if write.age.elapsed() > write.auto_save {
            write.age = Instant::now();
            do_auto_save = true;
        }
    }

    if do_auto_save {
        let (res, save_time) = timing(printer, "autosave", 100, || auto_save(printer, data));
        res?;

        // increase the wait for autosave. otherwise the savetime will be longer
        // than the interval at some point.
        let mut write = data.words.write()?;
        write.auto_save = save_time * 9;
    }

    Ok(())
}

pub fn auto_save(
    _printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    data: &'static Data,
) -> Result<(), AppError> {
    let tmp = Path::new(".tmp_stored");
    if tmp.exists() {
        return Ok(());
    }

    data.write(tmp)?;

    fs::rename(tmp, Path::new(".stored"))?;

    Ok(())
}

fn delete_file(
    _printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    data: &'static Data,
    file: String,
) -> Result<(), AppError> {
    let mut write = data.words.write()?;
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
    task: &str,
    res: Result<(), AppError>,
) {
    if let Err(err) = res {
        if let Ok(mut print) = printer.lock() {
            let _ = print.print(format!("{} {:?}", task, err));
        }
    }
}

pub fn timing<S: AsRef<str>, R>(
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    name: S,
    threshold: u64,
    fun: impl FnOnce() -> R,
) -> (R, Duration) {
    let now = Instant::now();

    let result = fun();

    let timing = now.elapsed();
    if timing > Duration::from_millis(threshold) {
        print_(printer, format!("{} {:?}", name.as_ref(), now.elapsed()));
    }

    (result, timing)
}
