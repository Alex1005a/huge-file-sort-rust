use std::{fs::{File, self}, io::{Read, Write, BufReader, BufRead}, collections::BinaryHeap, cmp::{Ordering, Reverse}, thread, sync::atomic::{AtomicI64, self}};
use std::str;
use chrono::{DateTime, Utc};
use crossbeam_channel::{bounded};
use itertools::Itertools;

pub struct ByFst<T, V>(pub T, pub V);

impl<T: Eq, V> Eq for ByFst<T, V> {}

impl<T: PartialEq, V> PartialEq for ByFst<T, V> {
    fn eq(&self, other: &ByFst<T, V>) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T: Ord, V>  PartialOrd for ByFst<T, V> {
    fn partial_cmp(&self, other: &ByFst<T, V>) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl<T: Ord, V> Ord for ByFst<T, V> {
    fn cmp(&self, other: &ByFst<T, V>) -> Ordering {
        self.0.cmp(&other.0)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Debug)]
pub struct Item(pub usize, pub Vec<u8>);

impl Ord for Item {
    fn cmp(&self, other: &Item) -> Ordering {
        let self_dot = self.0;
        let other_dot = other.0;
        let self_str = str::from_utf8(&self.1[self_dot + 2..]).unwrap();
        let other_str = str::from_utf8(&other.1[other_dot + 2..]).unwrap();
        let cmp_str = self_str.cmp(other_str);
        if cmp_str != Ordering::Equal { cmp_str }
        else {
            let self_num = str::from_utf8(&self.1[..self_dot]).unwrap().parse::<i32>().unwrap();
            let other_num = str::from_utf8(&other.1[..other_dot]).unwrap().parse::<i32>().unwrap();
            self_num.cmp(&other_num)
        }
    }
}

const NEWLINE_BYTE: u8 = b'\n';
const R_BYTE: u8 = b'\r';
const DOT_BYTE: u8 = b'.';

fn process_buffer(file: &mut File, buffer: &[u8]) {
    let split_buffer: Vec<Vec<u8>> = buffer.split(|&b| b == NEWLINE_BYTE || b == R_BYTE)
        .filter(|&b| !b.is_empty())
        .map(|b| b.to_vec())
        .collect();
        
    let mut sorted_strings =
        split_buffer
            .into_iter()
            .map(|b| Item((&b).into_iter().position(|&ch| ch == DOT_BYTE).unwrap(), b))
            .collect_vec();
    sorted_strings.sort_by(|a, b| a.cmp(b));
    
    let all_bytes = sorted_strings.iter().fold(Vec::new(), |mut vec, item: &Item| {
        vec.extend_from_slice(&item.1);
        vec.push(NEWLINE_BYTE);
        vec
    });

    file.write_all(&all_bytes).unwrap();
}

fn read_line_bytes(reader: &mut BufReader<File>, buffer: &mut Vec<u8>){
    reader.read_until(NEWLINE_BYTE, buffer).unwrap();
    if buffer.ends_with(&[NEWLINE_BYTE]) {
        buffer.pop();
        if buffer.ends_with(&[R_BYTE]) {
            buffer.pop();
        }
    }
}

fn main() {
    let file_path = "source.txt";
    let mut old_file = File::open(file_path).unwrap();

    const CAP: usize =  100 * 1024 * 1024;
    let mut v: Vec<u8> = vec![0; CAP];
    let buffer = v.as_mut_slice();

    let mut write_idx = 0;

    let start: DateTime<Utc> = Utc::now();

    let chunk_files = AtomicI64::new(1);
    let (tx, rx) = bounded::<Vec<u8>>(1);

    thread::scope(|s| {
        for _ in 1..3 {
            let rx = rx.clone();
            s.spawn(|| {
                for buf in rx {
                    let chunk_files = chunk_files.fetch_add(1, atomic::Ordering::Relaxed);
                    let mut output_file = File::create(format!("temp{}.tmp", chunk_files)).unwrap();
                    process_buffer(&mut output_file, &buf);
                }
            });
        }
        s.spawn(|| {
            loop {
                let length = old_file.read(&mut buffer[write_idx..]).unwrap();
                if length < CAP - write_idx {
                    tx.send((&buffer[..(write_idx + length)]).to_vec()).unwrap();
                    drop(tx);
                    break;
                }
        
                let read_until: usize;
        
                if let Some(idx) = buffer.into_iter().rposition(|&mut b| b == NEWLINE_BYTE || b == R_BYTE) {
                    write_idx = CAP - idx;
                    read_until = idx;
                } else {
                    write_idx = 0;
                    read_until = CAP;
                }
                
                tx.send((&buffer[..read_until]).to_vec()).unwrap();
                let mut remain = Vec::new();
                remain.write_all(&buffer[read_until..]).unwrap();
                buffer[..write_idx].clone_from_slice(&remain);
            }
        });
    });

    let mut heap = BinaryHeap::new();
    let chunk_files = chunk_files.load(atomic::Ordering::Relaxed);

    for i in 1 .. chunk_files {
        let temp_file = File::open(format!("temp{}.tmp", i)).unwrap();
        let mut reader = BufReader::new(temp_file);
        let mut buffer_vec = Vec::new();
        read_line_bytes(&mut reader, &mut buffer_vec);
        let dot_idx = (&buffer_vec).into_iter().position(|&ch| ch == DOT_BYTE).unwrap();
        heap.push(Reverse(ByFst(Item(dot_idx, buffer_vec), reader)));
    }
    
    write_idx = 0;

    let mut sorted_file = File::create("sorted.txt").unwrap();

    while let Some(Reverse(ByFst(write_bytes, mut reader))) = heap.pop() {
        let mut write_until = write_idx + write_bytes.1.len();
        if write_until >= CAP {
            sorted_file.write_all(&buffer[..write_idx]).unwrap();
            write_until = write_bytes.1.len();
            write_idx = 0;
        }
        buffer[write_idx..write_until].copy_from_slice(write_bytes.1.as_slice());
        buffer[write_until] = NEWLINE_BYTE;
        write_idx = write_until + 1;
        
        let mut buffer_vec = Vec::new();
        read_line_bytes(&mut reader, &mut buffer_vec);
        if !buffer_vec.is_empty() {
            heap.push(Reverse(ByFst(Item((&buffer_vec).into_iter().position(|&ch| ch == DOT_BYTE).unwrap(), buffer_vec), reader)));
        }
    }

    sorted_file.write_all(&buffer[..write_idx]).unwrap();

    let end: DateTime<Utc> = Utc::now();
    let duration = end - start;
    
    println!("{}", duration);
    println!("Sorting completed in {} seconds", duration.num_seconds());

    for i in 1 .. chunk_files {
        fs::remove_file(format!("temp{}.tmp", i)).unwrap();
    }
}
