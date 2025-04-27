# Video Merger Script

A Python script designed to scan a directory for subfolders containing video file parts (segments) and merge them into single video files using FFmpeg, but only if all parts within a folder share the same resolution.

## Features

* **Automatic Subdirectory Scanning:** Finds subdirectories within a specified source path.
* **Video File Identification:** Looks for `.mp4` and `.mkv` video files within each subdirectory.
* **Resolution Consistency Check:** Uses `ffprobe` to verify that all identified video parts within a single subdirectory have the same width and height.
* **Efficient Merging:** Uses `ffmpeg` with the `concat` demuxer and `-c copy` to merge video parts without re-encoding, preserving quality and maximizing speed.
* **Error Handling:**
    * Skips subdirectories where resolution is inconsistent across parts.
    * Skips subdirectories if the target merged output file already exists.
    * Handles errors during file operations and external command execution.
* **Detailed Logging:**
    * Outputs progress and status messages to the console.
    * Creates a timestamped log file (`video_merger_YYYYMMDD_HHMMSS.log`) within a `video-merger-logs` subdirectory (relative to the script location) containing detailed execution information.
    * Generates specific error log files (e.g., `FolderName_resolution_mismatch.log`, `FolderName_ffmpeg_error.log`) in the log directory for certain types of failures, making troubleshooting easier.

## Requirements

* **Python 3.x:** The script uses features like `pathlib` and type hints, typically requiring Python 3.6 or newer.
* **FFmpeg:** You **must** have FFmpeg installed on your system. The script relies on both the `ffmpeg` and `ffprobe` command-line tools being accessible in your system's PATH environment variable. (FFmpeg distributions usually include both).

## Usage

1.  **Save the Script:** Save the code as a Python file (e.g., `VideoMerger.py`).
2.  **Install FFmpeg:** Ensure FFmpeg is installed and accessible from your terminal/command prompt.
3.  **Prepare Your Videos:** Organize your video parts into subdirectories within a main source folder. Each subdirectory should contain the parts for *one* final merged video.
    ```
    MainSourceFolder/
    ├── VideoOne/
    │   ├── part1.mp4
    │   ├── part2.mp4
    │   └── part3.mp4
    ├── VideoTwo (Bonus)/
    │   ├── segment_A.mkv
    │   ├── segment_B.mkv
    └── _IgnoredFolder/
        └── some_video.mp4
    ```
4.  **Run the Script:** Open your terminal or command prompt, navigate to the directory where you saved the script, and run it using Python:
    ```bash
    python VideoMerger.py
    ```
5.  **Enter Paths:** The script will prompt you interactively:
    * First, enter the full path to your main source folder (e.g., `/path/to/MainSourceFolder`).
    * Second, enter the full path to the directory where you want the merged videos to be saved (e.g., `/path/to/MergedOutput`). This directory will be created if it doesn't exist.
6.  **Monitor Progress:** The script will log its progress to the console, indicating which folders it's processing, checking, merging, or skipping. Check the console output and the generated log files in the `video-merger-logs` directory for details.
7.  **Find Output:** Successfully merged videos will appear in the output directory you specified, named after their corresponding source subdirectory (e.g., `VideoOne.mp4`, `VideoTwo (Bonus).mkv`).

## How It Works

1.  The script starts and asks for the source and output directory paths.
2.  It initializes logging to both the console and a timestamped file.
3.  It checks if `ffmpeg` and `ffprobe` are available.
4.  It scans the source directory for subdirectories (ignoring names starting with `_` or `.`).
5.  For each valid subdirectory:
    * It finds all `.mp4` and `.mkv` files.
    * If videos are found, it uses `ffprobe` to get the resolution and duration of each part.
    * It checks if all parts have a positive duration and the same resolution.
    * If consistent and the output file doesn't already exist:
        * It creates a temporary text file listing the video parts for FFmpeg.
        * It executes `ffmpeg` using the `concat` demuxer and `-c copy` to merge the parts.
        * It verifies the duration of the merged file against the sum of parts (logging a warning if there's a significant difference).
    * If inconsistent, the output exists, or an error occurs, it logs the reason and skips merging for that folder.
6.  After processing all folders, it prints a summary of successes and failures.

## Limitations (Current Version)

* **File Extensions:** Only processes `.mp4` and `.mkv` files. Other formats are ignored.
* **Interactive Input Only:** Requires running the script interactively to provide paths; cannot be easily automated with command-line arguments.
* **No Overwrite:** If a merged file already exists in the output directory, the script will always skip that folder; there is no option to overwrite.
* **No Timeouts:** External `ffmpeg`/`ffprobe` commands might hang indefinitely on problematic files, causing the script to hang as well.
