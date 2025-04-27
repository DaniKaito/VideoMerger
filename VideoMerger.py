# -*- coding: utf-8 -*-
"""
This script finds subdirectories containing video parts (e.g., mp4, mkv files)
within a specified main directory. It checks if all parts within a subdirectory
have the same resolution. If they do, it merges them into a single video file
in the specified output directory using FFmpeg's concat demuxer.

Requires FFmpeg and ffprobe to be installed and accessible in the system PATH.
"""

import os
import subprocess
import logging
import shutil
import json
import tempfile
import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Union, Dict, Any

# --- Constants ---

# List of recognized video file extensions (case-insensitive check performed later).
VIDEO_EXTENSIONS: List[str] = ['mp4', 'mkv']

# --- Logging Setup ---

def setup_logging(log_file_path: Path) -> None:
    """
    Configures the application's logging.

    Sets up logging to both a file and the console. It attempts to create the
    log directory and file, handling potential permission errors gracefully.

    Args:
        log_file_path: The full path to the desired log file.
    """
    log_dir = log_file_path.parent
    try:
        # Attempt to create the log directory if it doesn't exist.
        log_dir.mkdir(parents=True, exist_ok=True)
        # Perform a quick write permission test in the log directory *before*
        # attempting to create the actual log file handler. This prevents
        # logger setup failures if directory creation succeeds but writing fails.
        test_file = log_dir / f".PERMISSION_TEST_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        test_file.touch()
        test_file.unlink()
    except OSError as e:
        # If directory creation or the permission test fails, log a critical error
        # to the console (as file logging isn't possible) and disable further logging.
        print(f"CRITICAL: Could not create or write to log directory {log_dir}. Logging disabled. Error: {e}")
        # Configure basic logging to only show CRITICAL messages if setup fails fundamentally.
        logging.basicConfig(level=logging.CRITICAL)
        logging.critical("Could not create or write to log directory %s: %s", log_dir, e)
        return # Stop further logging setup

    # Get the root logger instance.
    logger = logging.getLogger()
    # Set the root logger level to DEBUG to capture all messages.
    # Handlers will filter messages based on their own levels.
    logger.setLevel(logging.DEBUG)

    # Remove any pre-existing handlers attached to the root logger
    # to avoid duplicate logging if this function is called multiple times
    # or in an environment with existing logging configuration.
    if logger.hasHandlers():
        logger.handlers.clear()

    # --- File Handler ---
    file_handler = None # Initialize file_handler to None
    try:
        # Create a file handler to write logs to the specified file.
        # Use UTF-8 encoding for broad compatibility.
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        # Set the file handler to log INFO level messages and above.
        file_handler.setLevel(logging.INFO)
    except OSError as e:
        # If creating the log file fails (e.g., permissions, invalid path),
        # log a warning to the console and disable file logging.
        print(f"WARNING: Could not create log file {log_file_path}. File logging disabled. Error: {e}")
        # Log the warning using the basic config if file handler setup failed early.
        logging.warning("Could not create log file %s: %s", log_file_path, e)
        # Ensure file_handler remains None if creation failed

    # --- Console Handler ---
    # Create a console handler to display logs on the standard output.
    console_handler = logging.StreamHandler()
    # Set the console handler to log INFO level messages and above.
    console_handler.setLevel(logging.INFO)

    # --- Formatter ---
    # Define the log message format.
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Apply the formatter to the console handler.
    console_handler.setFormatter(formatter)
    # Apply the formatter to the file handler if it was created successfully.
    if file_handler:
        file_handler.setFormatter(formatter)

    # --- Add Handlers ---
    # Add the console handler to the root logger.
    logger.addHandler(console_handler)
    # Add the file handler to the root logger if it was created successfully.
    if file_handler:
        logger.addHandler(file_handler)

    # Log the successful setup details (for debugging purposes).
    logging.debug("Logging setup complete. File handler level: %s, Console handler level: %s",
                  logging.getLevelName(file_handler.level) if file_handler else "DISABLED",
                  logging.getLevelName(console_handler.level))
    # Inform the user where detailed logs are being saved (if file logging is enabled).
    if file_handler:
        logging.info(f"Logging detailed output to: {log_file_path}")
    else:
        logging.info("File logging is disabled due to an error.")


# --- Core Utilities ---

def _run_command(command: List[str], check: bool = True) -> subprocess.CompletedProcess:
    """
    Executes an external command using subprocess.run.

    This is a helper function intended for internal use (_ prefix). It captures
    stdout and stderr, logs the command execution, and handles common errors.

    Args:
        command: A list of strings representing the command and its arguments.
        check: If True (default), raises CalledProcessError if the command
               returns a non-zero exit code.

    Returns:
        A subprocess.CompletedProcess object containing the results.

    Raises:
        FileNotFoundError: If the command executable is not found.
        subprocess.CalledProcessError: If 'check' is True and the command fails.
        Exception: Other potential exceptions from subprocess.run.
    """
    # Log the command being executed for debugging.
    # list2cmdline is useful for seeing how the command list translates.
    logging.debug("Running command: %s", subprocess.list2cmdline(command))
    try:
        # Execute the command.
        result = subprocess.run(
            command,
            capture_output=True,       # Capture stdout and stderr.
            text=True,                 # Decode stdout/stderr as text.
            encoding='utf-8',          # Specify UTF-8 encoding.
            errors='replace',          # Replace decoding errors instead of crashing.
            check=check                # Raise error on non-zero exit code if True.
        )
        # Log captured output for debugging, stripping leading/trailing whitespace.
        stdout = result.stdout.strip() if result.stdout else ""
        stderr = result.stderr.strip() if result.stderr else ""
        if stdout:
            logging.debug("Command stdout:\n%s", stdout)
        if stderr:
            # Log stderr as debug even for successful commands, as some tools write info here.
            logging.debug("Command stderr:\n%s", stderr)
        return result
    except FileNotFoundError as e:
        # Handle error if the command executable (e.g., 'ffmpeg') isn't found.
        logging.error("Command not found: %s. Please ensure it's installed and in PATH.", command[0])
        raise e # Re-raise the exception to signal a critical failure.
    except subprocess.CalledProcessError as e:
        # Handle error if the command returns a non-zero exit code (and check=True).
        stderr = e.stderr.strip() if e.stderr else "N/A"
        logging.error("Command failed with exit code %d: %s", e.returncode, subprocess.list2cmdline(e.cmd))
        logging.error("Stderr:\n%s", stderr)
        raise e # Re-raise the exception.


def _get_video_metadata_ffprobe(video_path: Path) -> Optional[Dict[str, Any]]:
    """
    Retrieves essential video metadata (duration, width, height) using ffprobe.

    This helper function runs ffprobe on a video file, parses the JSON output,
    and extracts key information.

    Args:
        video_path: The Path object pointing to the video file.

    Returns:
        A dictionary containing 'duration' (float), 'width' (int), and
        'height' (int) if successful.
        Returns None if ffprobe fails, the output cannot be parsed, essential
        metadata is missing, or no video stream is found.

    Raises:
        FileNotFoundError: If the 'ffprobe' command is not found (propagated
                           from _run_command).
    """
    # Construct the ffprobe command to get format and stream info in JSON format.
    ffprobe_command = [
        "ffprobe",
        "-v", "quiet",             # Suppress informational messages from ffprobe.
        "-print_format", "json",   # Output format as JSON.
        "-show_format",            # Include container format information (duration).
        "-show_streams",           # Include stream information (codecs, resolution).
        str(video_path.resolve())  # Use the absolute path of the video file.
    ]

    try:
        # Run the ffprobe command using the helper function.
        result = _run_command(ffprobe_command, check=True)
        # Parse the JSON output from ffprobe's stdout.
        metadata = json.loads(result.stdout)

        # Find the first video stream in the metadata.
        video_stream = None
        for stream in metadata.get("streams", []):
            if stream.get("codec_type") == "video":
                video_stream = stream
                break # Found the first video stream, no need to check others.

        # If no video stream is found, log a warning and return None.
        if not video_stream:
            logging.warning("No video stream found in ffprobe output for: %s", video_path.name)
            return None

        # Extract duration, width, and height from the parsed metadata.
        # Duration is typically in the 'format' section.
        duration_str = metadata.get("format", {}).get("duration")
        # Width and height are in the video stream section.
        width = video_stream.get("width")
        height = video_stream.get("height")

        # Check if any essential metadata fields are missing.
        if duration_str is None or width is None or height is None:
            missing = [k for k, v in {'duration': duration_str, 'width': width, 'height': height}.items() if v is None]
            logging.warning("Missing metadata fields [%s] in ffprobe output for: %s", ', '.join(missing), video_path.name)
            return None # Indicate failure due to missing data.

        # Convert extracted values to the correct types and return as a dictionary.
        return {
            "duration": float(duration_str),
            "width": int(width),
            "height": int(height)
        }

    except subprocess.CalledProcessError:
        # Log a warning if ffprobe command failed (e.g., corrupted file).
        logging.warning("ffprobe failed for: %s. Could not get metadata.", video_path.name)
        return None
    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
        # Log a warning if JSON parsing or accessing expected keys/values fails.
        logging.warning("Error parsing ffprobe JSON output for %s: %s", video_path.name, e)
        return None
    except FileNotFoundError:
        # If ffprobe command wasn't found, re-raise the exception.
        # This is critical and should stop the process.
        raise


# --- File System Operations ---

def get_dirs(main_path: Path) -> List[Path]:
    """
    Finds relevant subdirectories within the main path.

    Scans the `main_path` and returns a sorted list of subdirectories
    that do not start with '_' or '.'.

    Args:
        main_path: The Path object of the directory to scan.

    Returns:
        A sorted list of Path objects representing the subdirectories to process.
        Returns an empty list if `main_path` is not a valid directory or
        if an error occurs during listing.
    """
    # Ensure the provided path is actually a directory.
    if not main_path.is_dir():
        logging.error("Main path is not a valid directory: %s", main_path)
        return []
    try:
        # List all items in the directory.
        # Filter for items that are directories AND whose names don't start
        # with typical 'hidden' or 'private' prefixes ('_' or '.').
        folder_list = [
            p for p in main_path.iterdir()
            if p.is_dir() and not p.name.startswith(('_', '.'))
        ]
        logging.info(f"Found {len(folder_list)} potential folders to process in {main_path}.")
        # Sort the list of directories alphabetically for consistent processing order.
        folder_list.sort()
        return folder_list
    except OSError as e:
        # Handle potential errors during directory listing (e.g., permissions).
        logging.error("Error reading directory %s: %s", main_path, e)
        return []


def get_videos(directory: Path) -> List[Path]:
    """
    Finds video files within a specific directory based on allowed extensions.

    Scans the `directory` and returns a sorted list of files matching
    the extensions defined in `VIDEO_EXTENSIONS`.

    Args:
        directory: The Path object of the directory to scan for videos.

    Returns:
        A sorted list of Path objects representing the video files found.
        Returns an empty list if `directory` is not valid or an error occurs.
    """
    # Ensure the provided path is a directory.
    if not directory.is_dir():
        logging.warning("Path is not a directory, cannot get videos: %s", directory)
        return []
    try:
        # List all items in the directory.
        # Filter for items that are files AND whose extension (converted to lowercase)
        # matches one of the extensions in VIDEO_EXTENSIONS.
        # file.suffix includes the dot (e.g., '.mp4'), so we slice [1:].
        video_array = [
            file for file in directory.iterdir()
            if file.is_file() and file.suffix.lower()[1:] in VIDEO_EXTENSIONS
        ]
        # Sort the video files alphabetically. This is important for the concat demuxer
        # which relies on the order in the input list file.
        video_array.sort()
        logging.info(f"Found {len(video_array)} video parts in {directory.name}")
        return video_array
    except OSError as e:
        # Handle potential errors during directory listing.
        logging.error("Error reading directory %s: %s", directory, e)
        return []


# --- Video Merging Logic ---

def merge_video(videos: List[Path], source_path: Path, output_dir: Path, log_dir: Path) -> bool:
    """
    Merges a list of video parts into a single file if checks pass.

    Checks for:
    1. Existence of the output file (skips if already merged).
    2. Resolution consistency across all video parts using ffprobe.
    3. Successful merging using FFmpeg's concat demuxer.
    4. Verifies the duration of the merged video against the sum of parts (within tolerance).

    Logs errors and creates specific error files in the log directory for certain failures
    (metadata error, resolution mismatch, ffmpeg error).

    Args:
        videos: A sorted list of Path objects for the video parts to merge.
        source_path: The Path object of the directory containing the video parts.
        output_dir: The Path object of the directory where the merged video will be saved.
        log_dir: The Path object of the directory for storing specific error logs.

    Returns:
        True if the video was successfully merged or already existed.
        False if any checks fail or an error occurs during merging.

    Raises:
        FileNotFoundError: If ffprobe or ffmpeg command is not found (propagated).
                           This is considered a fatal error for the script's operation.
    """
    # Basic check: If no video parts were found, there's nothing to merge.
    if not videos:
        logging.warning(f"No videos provided for merging in {source_path.name}.")
        return False

    # Determine the output filename based on the source folder name and the
    # extension of the first video part (assuming all parts have the same extension).
    folder_name = source_path.name
    file_extension = videos[0].suffix # e.g., '.mp4'
    video_output_path = output_dir / f"{folder_name}{file_extension}"

    # --- Check if Merged File Already Exists ---
    if video_output_path.exists():
        logging.info(f"Merged video already exists, skipping: {video_output_path.name}")
        # Consider existing file as success for this folder's processing.
        return True

    # --- Metadata Validation and Consistency Check ---
    total_duration: float = 0.0
    first_video_metadata: Optional[Dict[str, Any]] = None
    valid_videos_for_merge: List[Path] = [] # Store videos that pass checks
    resolutions_match = True # Flag to track resolution consistency

    logging.info(f"Checking resolution consistency for {len(videos)} video parts in {folder_name}...")

    try:
        # Iterate through each video part to get metadata and check consistency.
        for idx, video_path in enumerate(videos):
            logging.debug(f"Getting metadata for part {idx+1}: {video_path.name}")
            metadata = _get_video_metadata_ffprobe(video_path)

            # If metadata retrieval fails for any part, log an error, create a specific
            # error log file, mark resolution check as failed, and stop checking this folder.
            if metadata is None:
                logging.error(f"Failed to get metadata for {video_path.name}. Cannot verify consistency. Skipping folder.")
                error_log_path = log_dir / f"{folder_name}_metadata_error.log"
                try:
                    log_dir.mkdir(parents=True, exist_ok=True) # Ensure log dir exists
                    error_log_path.write_text(f"Failed to get metadata for video part: {video_path.resolve()}\n", encoding='utf-8')
                except OSError as log_e:
                    logging.error(f"Could not write metadata error log to {error_log_path}: {log_e}")
                resolutions_match = False
                break # Stop processing this folder

            # Extract resolution and duration for checks.
            current_resolution = (metadata['width'], metadata['height'])
            current_duration = metadata['duration']

            # Skip video parts with zero or negative duration, as they can cause issues.
            if current_duration <= 0:
                 logging.warning(f"Skipping video part with zero or negative duration: {video_path.name}")
                 continue # Move to the next video part

            # --- Resolution Check ---
            if first_video_metadata is None:
                # This is the first valid video part encountered. Store its metadata as reference.
                first_video_metadata = metadata
                logging.info(f"Reference resolution set from {video_path.name}: {current_resolution[0]}x{current_resolution[1]}")
            elif current_resolution != (first_video_metadata['width'], first_video_metadata['height']):
                # Resolution mismatch detected! Log details, create a specific error log,
                # mark resolution check as failed, and stop checking this folder.
                logging.error(f"Resolution mismatch in folder {folder_name}!")
                ref_name = valid_videos_for_merge[0].name if valid_videos_for_merge else videos[0].name
                logging.error(f"  Reference ({ref_name}): {first_video_metadata['width']}x{first_video_metadata['height']}")
                logging.error(f"  Mismatch ({video_path.name}): {metadata['width']}x{metadata['height']}")
                logging.error("Skipping merge for this folder due to resolution inconsistency.")
                error_log_path = log_dir / f"{folder_name}_resolution_mismatch.log"
                try:
                    log_dir.mkdir(parents=True, exist_ok=True) # Ensure log dir exists
                    error_log_path.write_text(
                        f"Resolution mismatch detected in folder: {source_path.resolve()}\n"
                        f"Reference video ({ref_name}): {first_video_metadata['width']}x{first_video_metadata['height']}\n"
                        f"Mismatch video ({video_path.name}): {metadata['width']}x{metadata['height']}\n"
                        f"Mismatch file path: {video_path.resolve()}\n",
                        encoding='utf-8'
                    )
                except OSError as log_e:
                    logging.error(f"Could not write resolution mismatch log to {error_log_path}: {log_e}")
                resolutions_match = False
                break # Stop processing this folder

            # If metadata is valid and resolution matches (or is the first video),
            # add it to the list of videos to merge and update the total duration.
            valid_videos_for_merge.append(video_path)
            total_duration += current_duration

        # If the resolution check failed at any point, return False for this folder.
        if not resolutions_match:
            return False

        # If after checking all parts, no valid videos are left (e.g., all had zero duration),
        # log an error and return False.
        if not valid_videos_for_merge:
            logging.error(f"No valid video parts found or processed in {folder_name} (e.g., zero duration). Cannot merge.")
            return False

        # Log a warning if some initial video parts were skipped.
        num_initial_videos = len(videos)
        num_valid_parts = len(valid_videos_for_merge)
        if num_valid_parts < num_initial_videos:
             logging.warning(f"Processed {num_valid_parts} out of {num_initial_videos} parts found in {folder_name}. Some parts may have been skipped (e.g., zero duration).")

        # Ensure we have reference metadata (needed for logging below). This should
        # usually be set, but handle the edge case where it might not be.
        if not first_video_metadata and valid_videos_for_merge:
            # Get metadata from the first valid part if it wasn't set during the loop
            # (This might happen if the very first video had zero duration but later ones were valid)
             first_video_metadata = _get_video_metadata_ffprobe(valid_videos_for_merge[0])

        # Log confirmation that the resolution check passed and the expected total duration.
        if first_video_metadata:
            logging.info(f"Resolution check passed for {folder_name}. All parts: {first_video_metadata['width']}x{first_video_metadata['height']}")
            logging.debug(f"Total calculated duration for {folder_name} from {num_valid_parts} valid parts: {total_duration:.2f} seconds.")
        else:
             # This is unlikely if valid_videos_for_merge is not empty, but handle defensively.
             logging.error(f"Could not determine reference resolution for {folder_name} despite having valid parts. Skipping.")
             return False


    except FileNotFoundError:
        # If ffprobe wasn't found during metadata checks, log critical error and return False.
        # No point continuing without ffprobe.
        logging.critical("ffprobe command not found. Cannot verify resolutions or merge videos.")
        # The exception would have been raised by _get_video_metadata_ffprobe, caught here.
        return False # Signal failure for this folder.
    except Exception as e:
        # Catch any other unexpected errors during the metadata check phase.
        logging.exception(f"An unexpected error occurred during metadata check for {folder_name}: {e}")
        return False

    # --- FFmpeg Merging Process ---
    list_file_path: Optional[Path] = None # Initialize path for the temporary list file
    try:
        # Create a temporary file to list the video parts for FFmpeg's concat demuxer.
        # 'delete=False' is important because we need to pass the file *path* to ffmpeg;
        # the file needs to exist until ffmpeg is done with it. We'll delete it manually in 'finally'.
        temp_f = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.txt', prefix=f"{folder_name}_ffmpeg_list_")
        list_file_path = Path(temp_f.name)
        with temp_f:
            # Write each valid video file path to the temporary file, one per line,
            # following the format required by the concat demuxer.
            # Use resolved (absolute) paths and replace backslashes for cross-platform compatibility.
            for video in valid_videos_for_merge:
                safe_path_str = str(video.resolve()).replace("\\", "/")
                temp_f.write(f"file '{safe_path_str}'\n")
            # The file is automatically closed when exiting the 'with' block.

        logging.debug(f"Generated temporary file list for ffmpeg: {list_file_path}")

        # Construct the ffmpeg command for merging.
        ffmpeg_command = [
            "ffmpeg",
            "-hide_banner",         # Suppress the default FFmpeg banner.
            "-v", "error",          # Only log errors from FFmpeg itself to stderr.
            "-f", "concat",         # Use the concat demuxer.
            "-safe", "0",           # Allow absolute paths in the list file (needed for resolved paths).
            "-i", str(list_file_path.resolve()), # Input is the temporary list file.
            "-c", "copy",           # Copy codecs directly without re-encoding (fast, preserves quality).
            str(video_output_path.resolve()) # Output path for the merged video.
        ]

        # --- Execute FFmpeg Command ---
        logging.info(f"Starting merge for {folder_name}...")
        _run_command(ffmpeg_command, check=True) # Run ffmpeg, check=True raises error on failure.

        logging.info(f"Successfully merged video saved to: {video_output_path.name}")

        # --- Verify Merged Video Duration (Optional but Recommended) ---
        logging.debug(f"Verifying duration of merged file: {video_output_path.name}")
        merged_metadata = _get_video_metadata_ffprobe(video_output_path)

        if merged_metadata is None or 'duration' not in merged_metadata or merged_metadata['duration'] is None:
            # If we can't get metadata for the *merged* file, log a warning.
            # This isn't necessarily a failure of the merge itself, but worth noting.
            logging.warning(f"Could not get or parse metadata for the *merged* video: {video_output_path.name}. Duration check skipped.")
        else:
            merged_duration = merged_metadata['duration']
            # Define tolerances for duration check (absolute and relative).
            # Merging can sometimes have tiny discrepancies.
            duration_tolerance_abs = 5.0  # seconds (e.g., for very short clips)
            duration_tolerance_rel = 0.02 # 2% (e.g., for longer videos)
            duration_diff = abs(total_duration - merged_duration)
            # Use the larger of the absolute or relative tolerance.
            max_allowed_diff = max(duration_tolerance_abs, total_duration * duration_tolerance_rel)

            logging.debug(f"Merged duration: {merged_duration:.2f}s, Sum of parts: {total_duration:.2f}s, Diff: {duration_diff:.2f}s, Max allowed diff: ~{max_allowed_diff:.2f}s")
            # If the difference exceeds the tolerance, log a warning.
            if duration_diff > max_allowed_diff:
                logging.warning(
                    f"Duration mismatch check FAILED for {folder_name}. "
                    f"Sum of parts duration: ~{total_duration:.2f}s, Merged duration: {merged_duration:.2f}s. "
                    f"Difference ({duration_diff:.2f}s) exceeds tolerance ({max_allowed_diff:.2f}s)."
                )
            else:
                logging.info(f"Duration check PASSED for {folder_name}.")

        # If we reached here without errors, the merge was successful.
        return True

    except subprocess.CalledProcessError as e:
        # Handle errors specifically from the ffmpeg merge command execution.
        logging.error(f"FFmpeg failed to merge videos for {folder_name}.")
        # Try to log ffmpeg's stderr output to a specific error file for diagnosis.
        error_log_path = log_dir / f"{folder_name}_ffmpeg_error.log"
        try:
            log_dir.mkdir(parents=True, exist_ok=True) # Ensure log dir exists
            # Write the captured stderr from the exception object.
            error_content = e.stderr or "No stderr captured from FFmpeg."
            error_log_path.write_text(error_content, encoding='utf-8')
            logging.error(f"FFmpeg error details saved to: {error_log_path}")
        except OSError as log_e:
            logging.error(f"Could not write ffmpeg error log to {error_log_path}: {log_e}")

        # Attempt to delete the potentially incomplete/corrupted output file if ffmpeg failed.
        try:
            if video_output_path.exists():
                video_output_path.unlink()
                logging.info(f"Deleted incomplete output file due to merge error: {video_output_path.name}")
        except OSError as del_e:
            # Log if deletion fails, but don't treat it as a primary error.
            logging.error(f"Could not delete incomplete output file {video_output_path.name}: {del_e}")
        return False # Signal merge failure

    except FileNotFoundError:
         # If ffmpeg command wasn't found, log critical error and return False.
         # This indicates a setup problem and should stop the script.
         logging.critical("ffmpeg command not found. Cannot merge videos.")
         # Re-raising might be appropriate here, but for robustness in processing folders,
         # we return False to allow the main loop to potentially continue with other folders.
         # The critical log should alert the user.
         return False # Signal failure for this folder.

    except Exception as e:
        # Catch any other unexpected errors during the merge process.
        logging.exception(f"An unexpected error occurred during the merge process for {folder_name}: {e}")
        # Attempt to delete potentially partial output file in case of unexpected errors too.
        if video_output_path.exists():
            try: video_output_path.unlink()
            except OSError: pass # Ignore deletion error here
        return False # Signal failure

    finally:
        # --- Cleanup Temporary File ---
        # This block ensures the temporary list file is deleted regardless of
        # whether the merge succeeded or failed.
        if list_file_path and list_file_path.exists():
            try:
                list_file_path.unlink()
                logging.debug(f"Deleted temporary file list: {list_file_path}")
            except OSError as e:
                # Log a warning if the temporary file cannot be deleted, but don't fail.
                logging.warning(f"Could not delete temporary file {list_file_path}: {e}")


# --- Main Execution Logic ---

def main(main_path_str: str, output_path_str: str) -> None:
    """
    Main function to orchestrate the video merging process.

    Sets up logging, checks for dependencies (FFmpeg/ffprobe), finds video folders,
    iterates through them, calls the merge function, and logs summary statistics.

    Args:
        main_path_str: String path to the main directory containing video subfolders.
        output_path_str: String path to the directory where merged videos should be saved.
    """
    # --- Determine Script Directory and Setup Logging ---
    try:
        # Get the directory where the script itself is located.
        # Used for placing the log directory relative to the script.
        script_dir = Path(__file__).parent.resolve()
    except NameError:
        # Fallback if __file__ is not defined (e.g., running in an interactive interpreter).
        # Use the current working directory instead.
        script_dir = Path.cwd()
        print(f"WARNING: Could not determine script directory via __file__, using current working directory: {script_dir}")

    # Define the log directory path relative to the script location.
    log_dir = script_dir / "video-merger-logs"
    # Create a timestamped log filename for the current run.
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_name = f"video_merger_{timestamp}.log"
    log_file_path = log_dir / log_file_name

    # Initialize the logging system.
    setup_logging(log_file_path)

    # --- Dependency Check ---
    # Check if ffmpeg and ffprobe executables are found in the system's PATH.
    ffmpeg_path = shutil.which("ffmpeg")
    ffprobe_path = shutil.which("ffprobe")

    if not ffmpeg_path:
        logging.critical("FFmpeg executable not found in system PATH. Please install FFmpeg and ensure it's accessible.")
        return # Stop execution if FFmpeg is missing.
    if not ffprobe_path:
        logging.critical("ffprobe executable not found in system PATH. Please install FFmpeg (which includes ffprobe) and ensure it's accessible.")
        return # Stop execution if ffprobe is missing.

    # --- Path Handling and Output Directory Creation ---
    try:
        # Resolve the input paths to absolute paths. Handles '.' and '..' etc.
        main_path = Path(main_path_str).resolve()
        output_path = Path(output_path_str).resolve()
    except Exception as e:
        # Catch potential errors during path resolution (e.g., invalid characters).
        logging.critical("Invalid main or output path provided. Could not resolve paths. Error: %s", e)
        return

    # Validate the main source path after resolution.
    if not main_path.is_dir():
         logging.critical(f"The specified main source path is not an existing directory: {main_path}")
         return

    try:
        # Create the output directory if it doesn't exist.
        # 'parents=True' creates any necessary parent directories.
        # 'exist_ok=True' prevents an error if the directory already exists.
        output_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        # Handle errors during output directory creation (e.g., permissions).
        logging.critical("Could not create output directory '%s': %s", output_path, e)
        return

    # --- Log Initial Setup ---
    logging.info("--- Video Merging Script Started ---")
    logging.info("Script location: %s", script_dir)
    logging.info("Using ffmpeg: %s", ffmpeg_path)
    logging.info("Using ffprobe: %s", ffprobe_path)
    logging.info("Main video source path: %s", main_path)
    logging.info("Output path for merged videos: %s", output_path)
    logging.info("Log directory: %s", log_dir) # Log dir for specific errors and main log

    # --- Process Folders ---
    # Get the list of subdirectories to process within the main path.
    folder_paths = get_dirs(main_path)
    # Initialize counters for summary statistics.
    processed_count = 0
    success_count = 0
    fail_count = 0

    # Iterate through each identified folder path.
    for path in folder_paths:
        # Check again for underscore/dot prefixes ( belt-and-suspenders with get_dirs filter)
        if path.name.startswith(('_', '.')):
            logging.debug("Skipping directory explicitly: %s", path.name)
            continue

        # Log separator for clarity between folder processing.
        logging.info("="*40)
        logging.info(f"Processing folder: {path.name}")
        processed_count += 1
        # Find all relevant video files within the current folder.
        videos_arr = get_videos(path)

        # If no video files are found in the folder, log a warning and skip to the next folder.
        if not videos_arr:
            logging.warning(f"No video files found matching extensions {VIDEO_EXTENSIONS} in {path.name}, skipping.")
            fail_count += 1 # Count as failed/skipped for this folder.
            continue

        # --- Attempt to Merge Videos ---
        try:
            # Call the main merging logic function for the current folder's videos.
            success = merge_video(videos_arr, path, output_path, log_dir)
            # Update counters based on the result.
            if success:
                success_count += 1
            else:
                fail_count += 1
        except FileNotFoundError:
             # This handles the case where ffmpeg/ffprobe are not found during merge_video
             # This should have been caught earlier, but handle defensively in the loop.
             logging.critical(f"Halting processing due to missing ffmpeg/ffprobe during processing of {path.name}.")
             # Stop processing further folders if critical dependencies are missing.
             fail_count += 1
             break # Exit the loop
        except Exception as e:
            # Catch any unexpected exceptions during the processing of a single folder.
            # Log the error and continue to the next folder if possible.
            logging.exception(f"Unhandled exception occurred while processing folder {path.name}: {e}")
            fail_count += 1
            # Optionally: Decide whether to continue or break the loop on unexpected errors.
            # Currently continues to the next folder.

        logging.info(f"Finished processing folder: {path.name}")
        # Add a newline for better readability in the log.
        logging.info("="*40 + "\n")


    # --- Log Summary ---
    logging.info("--- Video Merging Script Finished ---")
    logging.info(f"Total potential folders found (excluding '_', '.' prefixes): {len(folder_paths)}")
    logging.info(f"Folders attempted processing: {processed_count}")
    logging.info(f"Successfully merged (or skipped existing): {success_count}")
    logging.info(f"Failed/Skipped (errors, no videos, mismatch, etc.): {fail_count}")

    # --- Check for Specific Error Logs ---
    try:
        # Check if the log directory itself exists (it might not if no file logging
        # was possible and no specific error files were created).
        if not log_dir.exists():
            logging.info("No 'video-merger-logs' directory was created (implies no file logging and no specific folder errors requiring log files).")
        else:
            # Check if any files *other* than the main '.log' files were created in the log directory.
            # These indicate specific errors like metadata/resolution/ffmpeg failures.
            error_files = [
                item for item in log_dir.iterdir()
                if item.is_file() and not item.name.startswith('video_merger_') and item.suffix.lower() == '.log'
                # Example filter: find files like 'foldername_ffmpeg_error.log'
                # Adjust filter based on actual error file naming convention used in merge_video
                # This example looks for any .log file not starting with the main log prefix.
                # A more specific filter might be `and '_' in item.stem and item.stem.endswith(('_error', '_mismatch'))`
            ]
            other_error_files = [item for item in log_dir.iterdir() if item.is_file() and item.suffix != '.log'] # Check for non-log files too?

            total_specific_logs = len(error_files) # Count only the specific .log files

            if not total_specific_logs:
                logging.info("Main log file(s) created. No specific error log files generated for individual folders.")
            else:
                logging.warning(f"Found {total_specific_logs} specific error log file(s) in the log directory, indicating issues with certain folders.")
                logging.warning(f"Please review files in: {log_dir}")
    except OSError as e:
        # Handle potential errors when trying to list the log directory contents.
        logging.warning(f"Could not check contents of log directory {log_dir}: {e}")


# --- Script Entry Point ---

# Standard Python construct: Ensures the code inside only runs when the script
# is executed directly (not when imported as a module).
if __name__ == "__main__":
    # Prompt the user for the required input paths.
    main_path_input = input("Enter the full path to the main folder containing video subfolders:\n> ")
    output_path_input = input("Enter the full path for the output directory where merged videos will be saved:\n> ")

    # Basic validation: Ensure paths were actually entered.
    if not main_path_input or not output_path_input:
        print("\nError: Both the main source path and the output path are required.")
    else:
        # Perform a basic check if the main path entered exists as a directory *before* calling main().
        # This provides quicker feedback to the user for a common mistake.
        main_p_check = Path(main_path_input)
        if not main_p_check.is_dir():
             print(f"\nError: The specified main source path does not exist or is not a directory:")
             print(f"'{main_path_input}'")
        else:
            # If inputs seem okay, call the main function to start the process.
            main(main_path_input, output_path_input)

    # Keep the console window open after the script finishes until the user presses Enter.
    # This allows users running the script by double-clicking to see the output.
    input("\nProcessing complete. Press Enter to exit...")
