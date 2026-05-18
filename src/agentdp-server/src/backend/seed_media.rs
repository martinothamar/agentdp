use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;

const BYTES_PER_SECTOR: usize = 512;
const SECTORS_PER_CLUSTER: usize = 1;
const RESERVED_SECTORS: usize = 1;
const FAT_COUNT: usize = 2;
const ROOT_ENTRY_COUNT: usize = 512;
const ROOT_DIRECTORY_SECTORS: usize = ROOT_ENTRY_COUNT * 32 / BYTES_PER_SECTOR;
const MIN_TOTAL_SECTORS: usize = 8192;
const END_OF_CHAIN: u16 = 0xFFFF;
const VOLUME_LABEL: &[u8; 11] = b"CIDATA     ";

#[derive(Debug, Error)]
pub enum Error {
    #[error("cloud-init seed media contents are too large")]
    TooLarge,
    #[error("failed to write cloud-init seed media {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

struct SeedFile<'a> {
    long_name: &'static str,
    short_name: [u8; 11],
    contents: &'a str,
}

struct Layout {
    total_sectors: usize,
    fat_sectors: usize,
    data_start_sector: usize,
}

/// Writes a FAT `CIDATA` cloud-init seed image.
///
/// # Errors
///
/// Returns an error if the seed contents are too large or the image cannot be
/// written to disk.
pub fn write(path: &Path, meta_data: &str, user_data: &str) -> Result<(), Error> {
    let files = [
        SeedFile {
            long_name: "meta-data",
            short_name: *b"METADA~1   ",
            contents: meta_data,
        },
        SeedFile {
            long_name: "user-data",
            short_name: *b"USERDA~1   ",
            contents: user_data,
        },
    ];
    let image = build_image(&files)?;
    fs::write(path, image).map_err(|source| Error::Write {
        path: path.to_path_buf(),
        source,
    })
}

fn build_image(files: &[SeedFile<'_>]) -> Result<Vec<u8>, Error> {
    let needed_clusters = files
        .iter()
        .map(|file| cluster_count(file.contents.len()))
        .sum::<usize>();
    let layout = layout_for(needed_clusters)?;
    let mut image = vec![0; layout.total_sectors * BYTES_PER_SECTOR];

    write_boot_sector(&mut image, &layout)?;
    let chains = write_file_data(&mut image, &layout, files);
    write_fats(&mut image, &layout, &chains);
    write_root_directory(&mut image, &layout, files, &chains)?;
    Ok(image)
}

fn layout_for(needed_clusters: usize) -> Result<Layout, Error> {
    let mut total_sectors = MIN_TOTAL_SECTORS;
    loop {
        let mut fat_sectors = 1;
        loop {
            let data_sectors = total_sectors
                .checked_sub(RESERVED_SECTORS + ROOT_DIRECTORY_SECTORS + FAT_COUNT * fat_sectors)
                .ok_or(Error::TooLarge)?;
            let data_cluster_count = data_sectors / SECTORS_PER_CLUSTER;
            let required_fat_bytes = (data_cluster_count + 2) * 2;
            let required_fat_sectors = required_fat_bytes.div_ceil(BYTES_PER_SECTOR);
            if required_fat_sectors == fat_sectors {
                let data_start_sector = RESERVED_SECTORS + FAT_COUNT * fat_sectors + ROOT_DIRECTORY_SECTORS;
                if data_cluster_count >= 4085 && data_cluster_count >= needed_clusters {
                    return Ok(Layout {
                        total_sectors,
                        fat_sectors,
                        data_start_sector,
                    });
                }
                break;
            }
            fat_sectors = required_fat_sectors;
        }

        total_sectors = total_sectors.checked_mul(2).ok_or(Error::TooLarge)?;
        if total_sectors > u16::MAX.into() {
            return Err(Error::TooLarge);
        }
    }
}

fn write_boot_sector(image: &mut [u8], layout: &Layout) -> Result<(), Error> {
    let sector = &mut image[..BYTES_PER_SECTOR];
    sector[0..3].copy_from_slice(&[0xEB, 0x3C, 0x90]);
    sector[3..11].copy_from_slice(b"MSDOS5.0");
    write_u16(sector, 11, to_u16(BYTES_PER_SECTOR)?);
    sector[13] = to_u8(SECTORS_PER_CLUSTER)?;
    write_u16(sector, 14, to_u16(RESERVED_SECTORS)?);
    sector[16] = to_u8(FAT_COUNT)?;
    write_u16(sector, 17, to_u16(ROOT_ENTRY_COUNT)?);
    write_u16(sector, 19, to_u16(layout.total_sectors)?);
    sector[21] = 0xF8;
    write_u16(sector, 22, to_u16(layout.fat_sectors)?);
    write_u16(sector, 24, 32);
    write_u16(sector, 26, 64);
    sector[36] = 0x80;
    sector[38] = 0x29;
    sector[39..43].copy_from_slice(&0xA6D0_0001_u32.to_le_bytes());
    sector[43..54].copy_from_slice(b"CIDATA     ");
    sector[54..62].copy_from_slice(b"FAT16   ");
    sector[510] = 0x55;
    sector[511] = 0xAA;
    Ok(())
}

fn write_file_data(image: &mut [u8], layout: &Layout, files: &[SeedFile<'_>]) -> Vec<Vec<u16>> {
    let mut next_cluster = 2_u16;
    let mut chains = Vec::with_capacity(files.len());
    for file in files {
        let mut chain = Vec::new();
        for chunk in file.contents.as_bytes().chunks(BYTES_PER_SECTOR * SECTORS_PER_CLUSTER) {
            let cluster = next_cluster;
            next_cluster += 1;
            let offset = cluster_offset(layout, cluster);
            image[offset..offset + chunk.len()].copy_from_slice(chunk);
            chain.push(cluster);
        }
        chains.push(chain);
    }
    chains
}

fn write_fats(image: &mut [u8], layout: &Layout, chains: &[Vec<u16>]) {
    for fat_index in 0..FAT_COUNT {
        let fat_start = (RESERVED_SECTORS + fat_index * layout.fat_sectors) * BYTES_PER_SECTOR;
        let fat_end = fat_start + layout.fat_sectors * BYTES_PER_SECTOR;
        let fat = &mut image[fat_start..fat_end];
        write_fat_entry(fat, 0, 0xFFF8);
        write_fat_entry(fat, 1, END_OF_CHAIN);
        for chain in chains {
            for (index, cluster) in chain.iter().enumerate() {
                let next = chain.get(index + 1).copied().unwrap_or(END_OF_CHAIN);
                write_fat_entry(fat, *cluster, next);
            }
        }
    }
}

fn write_root_directory(
    image: &mut [u8],
    layout: &Layout,
    files: &[SeedFile<'_>],
    chains: &[Vec<u16>],
) -> Result<(), Error> {
    let root_start = (RESERVED_SECTORS + FAT_COUNT * layout.fat_sectors) * BYTES_PER_SECTOR;
    let mut entry_offset = root_start;

    write_volume_label_entry(&mut image[entry_offset..entry_offset + 32]);
    entry_offset += 32;

    for (file, chain) in files.iter().zip(chains) {
        let checksum = lfn_checksum(&file.short_name);
        write_lfn_entry(&mut image[entry_offset..entry_offset + 32], file.long_name, checksum)?;
        entry_offset += 32;
        write_short_entry(
            &mut image[entry_offset..entry_offset + 32],
            &file.short_name,
            chain.first().copied().unwrap_or(0),
            file.contents.len(),
        )?;
        entry_offset += 32;
    }

    Ok(())
}

fn write_volume_label_entry(entry: &mut [u8]) {
    entry[0..11].copy_from_slice(VOLUME_LABEL);
    entry[11] = 0x08;
}

fn write_lfn_entry(entry: &mut [u8], name: &str, checksum: u8) -> Result<(), Error> {
    let mut name_units = name.encode_utf16().collect::<Vec<_>>();
    if name_units.len() > 13 {
        return Err(Error::TooLarge);
    }
    name_units.push(0);
    while name_units.len() < 13 {
        name_units.push(0xFFFF);
    }

    entry[0] = 0x41;
    entry[11] = 0x0F;
    entry[13] = checksum;
    entry[26] = 0;
    entry[27] = 0;
    write_lfn_units(entry, &[1, 3, 5, 7, 9], &name_units[0..5]);
    write_lfn_units(entry, &[14, 16, 18, 20, 22, 24], &name_units[5..11]);
    write_lfn_units(entry, &[28, 30], &name_units[11..13]);
    Ok(())
}

fn write_short_entry(entry: &mut [u8], short_name: &[u8; 11], first_cluster: u16, len: usize) -> Result<(), Error> {
    let len = u32::try_from(len).map_err(|_| Error::TooLarge)?;
    entry[0..11].copy_from_slice(short_name);
    entry[11] = 0x20;
    write_u16(entry, 26, first_cluster);
    write_u32(entry, 28, len);
    Ok(())
}

fn write_lfn_units(entry: &mut [u8], offsets: &[usize], units: &[u16]) {
    for (offset, unit) in offsets.iter().zip(units) {
        write_u16(entry, *offset, *unit);
    }
}

fn write_fat_entry(fat: &mut [u8], cluster: u16, value: u16) {
    write_u16(fat, usize::from(cluster) * 2, value);
}

fn cluster_offset(layout: &Layout, cluster: u16) -> usize {
    let data_sector = layout.data_start_sector + (usize::from(cluster) - 2) * SECTORS_PER_CLUSTER;
    data_sector * BYTES_PER_SECTOR
}

fn cluster_count(len: usize) -> usize {
    len.div_ceil(BYTES_PER_SECTOR * SECTORS_PER_CLUSTER).max(1)
}

fn lfn_checksum(short_name: &[u8; 11]) -> u8 {
    short_name.iter().fold(0_u8, |sum, byte| {
        ((sum & 1) << 7).wrapping_add(sum >> 1).wrapping_add(*byte)
    })
}

fn write_u16(buffer: &mut [u8], offset: usize, value: u16) {
    buffer[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(buffer: &mut [u8], offset: usize, value: u32) {
    buffer[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn to_u16(value: usize) -> Result<u16, Error> {
    u16::try_from(value).map_err(|_| Error::TooLarge)
}

fn to_u8(value: usize) -> Result<u8, Error> {
    u8::try_from(value).map_err(|_| Error::TooLarge)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::backend::seed_media;
    use crate::backend::seed_media::BYTES_PER_SECTOR;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn writes_cidata_fat_seed_image() {
        let temp = TestTempDir::create("seed-media");
        let path = temp.path().join("seed.img");

        seed_media::write(&path, "instance-id: test\n", "#cloud-config\npackages: []\n").unwrap();

        let image = fs::read(path).unwrap();
        assert_eq!(image.len(), 4 * 1024 * 1024);
        assert_eq!(&image[43..54], b"CIDATA     ");
        assert_eq!(&image[54..62], b"FAT16   ");
        let root_start = root_directory_start(&image);
        assert_eq!(&image[root_start..root_start + 11], b"CIDATA     ");
        assert_eq!(image[root_start + 11], 0x08);
        assert!(contains_bytes(&image, b"instance-id: test\n"));
        assert!(contains_bytes(&image, b"#cloud-config\npackages: []\n"));
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn create(name: &str) -> Self {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("agentdp-{name}-{}-{timestamp}-{id}", std::process::id()));
            fs::create_dir(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _result = fs::remove_dir_all(&self.path);
        }
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|window| window == needle)
    }

    fn root_directory_start(image: &[u8]) -> usize {
        let reserved_sectors = usize::from(read_u16(image, 14));
        let fat_count = usize::from(image[16]);
        let fat_sectors = usize::from(read_u16(image, 22));
        (reserved_sectors + fat_count * fat_sectors) * BYTES_PER_SECTOR
    }

    fn read_u16(image: &[u8], offset: usize) -> u16 {
        u16::from_le_bytes([image[offset], image[offset + 1]])
    }
}
