-- Add explicit analysis anchor fields for friendlier querying / backtest
-- Safe to run multiple times.

ALTER TABLE public.analysis_history
ADD COLUMN IF NOT EXISTS analysis_date date;

ALTER TABLE public.analysis_history
ADD COLUMN IF NOT EXISTS analysis_close double precision;

CREATE INDEX IF NOT EXISTS ix_analysis_history_analysis_date
ON public.analysis_history (analysis_date);

-- Backfill from context_snapshot JSON when possible.
-- Supports both new keys:
--   { "analysis_date": "YYYY-MM-DD", "analysis_close": ... }
-- and legacy nested keys:
--   { "enhanced_context": { "date": "YYYY-MM-DD", "today": { "close": ... } } }
UPDATE public.analysis_history
SET
  analysis_date = COALESCE(
    analysis_date,
    NULLIF(context_snapshot::jsonb ->> 'analysis_date', '')::date,
    NULLIF(context_snapshot::jsonb -> 'enhanced_context' ->> 'date', '')::date
  ),
  analysis_close = COALESCE(
    analysis_close,
    NULLIF(context_snapshot::jsonb ->> 'analysis_close', '')::double precision,
    NULLIF(context_snapshot::jsonb -> 'enhanced_context' -> 'today' ->> 'close', '')::double precision
  )
WHERE context_snapshot IS NOT NULL
  AND context_snapshot <> '';
