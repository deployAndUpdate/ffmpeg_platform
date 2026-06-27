-- Named transcode preset used when creating a job (resolved ffmpeg_args stored as snapshot).

ALTER TABLE jobs
ADD COLUMN preset TEXT;
