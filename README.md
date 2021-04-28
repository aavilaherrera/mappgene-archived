# mappgene

Massively Parallel and Portable Genomic Sequence Analysis
<br></br><br></br>

## Installation

`git clone https://github.com/LLNL/mappgene.git`

`pip3 install --user parsl`
<br></br><br></br>

## Running

### Prep

1. Copy your reference genome in `fasta` format to `vpipe_files/references/`.
2. Inspect and configure `vpipe_files/vpipe.config` for your run (e.g.,
   `reference = `, `trim_percent_cutoff = `, `threads = `)
3. Organize your input files (typically gzip compressed `fastq` formatted
   paired-end reads). `mappgene.py` expects the following layout, where sample
   names are taken from the subdirectories (i.e., foo, bar, baz):

    ```
    /path/to/input_dirs
    |-- foo
    |   |-- foo_R1.fastq.gz
    |   `-- foo_R2.fastq.gz
    |-- bar
    |   |-- bar_R1.fastq.gz
    |   `-- bar_R2.fastq.gz
    |-- baz
    |   |...
    ...
    ```

### Run

<b>Specify arguments</b>

Example:

```bash
python3 mappgene.py \
    --input_dirs /path/to/input_dirs \
    --output_dirs /path/to/output_dirs \
    --read_length 130 \
    --nnodes 16 \
    --walltime 12:59:59
```

**OR**

`python3 mappgene.py <config_json>`
<br></br><br></br>

<b>More info</b>

`python3 mappgene.py --help`

## License

MaPPeRTrac is distributed under the terms of the BSD-3 License.

LLNL-CODE-821512
