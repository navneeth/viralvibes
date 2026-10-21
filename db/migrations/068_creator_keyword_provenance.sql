ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS transcript_keywords_metadata JSONB DEFAULT NULL;

COMMENT ON COLUMN public.creators.transcript_keywords_metadata IS
    'Optional versioned provenance for transcript_keywords. Schema version 1: '
    'schema_version, source (transcript/description/mixed/unknown), computed_at '
    '(UTC extraction time), extractor_version, model_id, model_revision, '
    'content_language, language_confidence, sampled_video_count, '
    'transcript_video_count, description_video_count, accepted_word_count. '
    'NULL means provenance unavailable; never infer source from word counts. '
    'Do not include transcript bodies, credentials, or private user context.';

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'creators'
  AND column_name = 'transcript_keywords_metadata';
