use nix::unistd::SysconfVar;
use std::arch::x86_64::_rdtsc;
use std::collections::HashMap;
use std::fs::File;
use std::io::stdout;
use std::io::Write;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd};
use std::ptr::null_mut;

#[inline]
fn read_cpu_timer() -> u64 {
    unsafe { _rdtsc() }
}

#[inline]
fn get_os_clock_frequency() -> u64 {
    nix::unistd::sysconf(SysconfVar::CLK_TCK).unwrap().unwrap() as u64 * 10_000
}

#[inline]
fn read_os_timer() -> u64 {
    let mut value = nix::libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    };

    let call_res = unsafe { nix::libc::gettimeofday(&mut value, null_mut()) };
    assert_ne!(call_res, -1, "gettimeofday() failed");
    get_os_clock_frequency() * value.tv_sec as u64 + value.tv_usec as u64
}

#[inline]
fn get_cpu_frequency() -> u64 {
    let ms_to_wait = 100u64;
    let os_freq = get_os_clock_frequency();

    let cpu_start = read_cpu_timer();
    let os_start = read_os_timer();
    let mut os_elapsed = 0u64;
    let os_wait_time = os_freq * ms_to_wait / 1000;

    while os_elapsed < os_wait_time {
        os_elapsed = read_os_timer() - os_start;
    }

    let cpu_end = read_cpu_timer();
    let cpu_elapsed = cpu_end - cpu_start;

    assert_ne!(os_elapsed, 0, "os elapsed is zero!");
    os_freq * cpu_elapsed / os_elapsed
}

const MAX_PROFILE_ANCHORS: usize = 4096;
const PROFILE_OUTPUT_ENV: &str = "PROFILE_OUT";

#[derive(Default)]
pub struct ProfileAnchor {
    tsc_elapsed_exclusive: u64,
    tsc_elapsed_inclusive: u64,
    num_hits: u64,
    bytes_processed: u64,
    label: String,
}

pub struct ProfileBlock {
    start_tsc: u64,
    old_tsc_inclusive: u64,
    anchor_index: usize,
    parent_index: usize,
    bytes_processed: u64,
    label: String,
    profiler_addr: usize,
}

impl ProfileBlock {
    pub fn new(
        anchor_index: usize,
        label: &str,
        bytes_processed: u64,
        profiler: *mut Profiler,
    ) -> Self {
        let profiler_mut = unsafe { profiler.as_mut() }.unwrap();
        let old_tsc_inclusive = profiler_mut.anchors[anchor_index].tsc_elapsed_inclusive;
        let parent_index = profiler_mut.parent_index;
        profiler_mut.parent_index = anchor_index;

        Self {
            start_tsc: read_cpu_timer(),
            old_tsc_inclusive,
            parent_index,
            anchor_index,
            label: label.to_string(),
            bytes_processed,
            profiler_addr: profiler as usize,
        }
    }
}

impl Drop for ProfileBlock {
    fn drop(&mut self) {
        let profiler_mut =
            unsafe { (self.profiler_addr as *const Profiler).cast_mut().as_mut() }.unwrap();
        profiler_mut.parent_index = self.parent_index;

        let anchor = &mut profiler_mut.anchors[self.anchor_index];

        let elapsed = read_cpu_timer() - self.start_tsc;

        anchor.tsc_elapsed_exclusive += elapsed;
        anchor.tsc_elapsed_inclusive = self.old_tsc_inclusive + elapsed;
        anchor.bytes_processed += self.bytes_processed;
        anchor.num_hits += 1;
        anchor.label = self.label.clone();

        let parent_anchor = &mut profiler_mut.anchors[self.parent_index];
        parent_anchor.tsc_elapsed_exclusive -= elapsed;
    }
}

pub struct Profiler {
    anchors: [ProfileAnchor; MAX_PROFILE_ANCHORS],
    label_to_index: HashMap<String, usize>,
    parent_index: usize,
    start_tsc: u64,
    end_tsc: u64,
    log_file: File,
}

fn empty_anchores() -> [ProfileAnchor; MAX_PROFILE_ANCHORS] {
    let mut anchor_array: [MaybeUninit<ProfileAnchor>; MAX_PROFILE_ANCHORS] =
        unsafe { MaybeUninit::uninit().assume_init() };

    for v in anchor_array.iter_mut() {
        *v = MaybeUninit::new(ProfileAnchor::default());
    }

    unsafe { std::mem::transmute(anchor_array) }
}

impl Profiler {
    pub fn new() -> Self {
        let profile_output = if let Ok(value) = std::env::var(PROFILE_OUTPUT_ENV) {
            File::create(value).unwrap()
        } else {
            unsafe { File::from_raw_fd(stdout().as_raw_fd()) }
        };

        Profiler {
            anchors: empty_anchores(),
            label_to_index: HashMap::new(),
            log_file: profile_output,
            parent_index: 0,
            start_tsc: 0,
            end_tsc: 0,
        }
    }

    #[inline]
    pub fn start(&mut self) {
        self.anchors = empty_anchores();
        self.label_to_index.clear();
        self.parent_index = 0;
        self.end_tsc = 0;
        self.start_tsc = read_cpu_timer();
    }

    pub fn print_results(&mut self) {
        let cpu_freq = get_cpu_frequency();
        assert!(cpu_freq > 0);

        let end_tsc = if self.end_tsc != 0 {
            self.end_tsc
        } else {
            read_cpu_timer()
        };

        let total_cpu_elapsed = end_tsc - self.start_tsc;
        let _ = writeln!(self.log_file, "Performance report:");
        let _ = writeln!(self.log_file, "    CPU frequency: {cpu_freq}hz");
        let _ = writeln!(
            self.log_file,
            "    Total time = {:.4}ms",
            1000.0 * total_cpu_elapsed as f64 / cpu_freq as f64
        );

        for anchor in self.anchors.iter().skip(1) {
            if anchor.tsc_elapsed_exclusive != 0 && anchor.num_hits != 0 {
                let ms_elapsed = 1000.0 * anchor.tsc_elapsed_exclusive as f64 / cpu_freq as f64;
                let percentage =
                    100.0 * (anchor.tsc_elapsed_exclusive as f64 / total_cpu_elapsed as f64);

                let _ = write!(
                    self.log_file,
                    "{}[{}]: {ms_elapsed:.10}ms ({percentage:.2}%",
                    anchor.label, anchor.num_hits
                );

                if anchor.tsc_elapsed_exclusive != anchor.tsc_elapsed_inclusive {
                    let percent_with_children =
                        100.0 * (anchor.tsc_elapsed_inclusive as f64 / total_cpu_elapsed as f64);
                    let _ = write!(self.log_file, ", {percent_with_children:.2}% w/children");
                }
                let _ = write!(self.log_file, ")");

                if anchor.bytes_processed != 0 {
                    let mb = 1024.0 * 1024.0;
                    let gb = mb * 1024.0;

                    let seconds = anchor.tsc_elapsed_inclusive as f64 / cpu_freq as f64;
                    let bytes_per_second = anchor.bytes_processed as f64 / seconds;
                    let megabytes = anchor.bytes_processed as f64 / mb;
                    let gigabytes_per_second = bytes_per_second / gb;

                    let _ = write!(
                        self.log_file,
                        " {megabytes:.3}MBs at {gigabytes_per_second:.2}GB/s"
                    );
                }

                let _ = writeln!(self.log_file);
            }
        }
    }

    #[inline]
    pub fn begin_block_with_bandwidth(&mut self, label: &str, bytes: u64) -> ProfileBlock {
        let current_index = self.label_to_index.len() + 1;
        let index = *self
            .label_to_index
            .entry(label.to_string())
            .or_insert(current_index);
        ProfileBlock::new(index, label, bytes, self)
    }

    #[inline]
    pub fn begin_block(&mut self, label: &str) -> ProfileBlock {
        self.begin_block_with_bandwidth(label, 0)
    }

    #[inline]
    pub fn end_and_print_results(&mut self) {
        self.end_tsc = read_cpu_timer();
        self.print_results();
    }
}
