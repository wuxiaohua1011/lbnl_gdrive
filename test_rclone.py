from g_drive.utilities import run_command
cmd = "rclone --multi-thread-streams=5 copy -P /Volumes/KESU/lbnl_data/output remote: "
run_command(cmd)