-- Revert FileDimension:feature/auto_update_timestamp from pg

BEGIN;

-- Drop the trigger from the table
DROP TRIGGER IF EXISTS trigger_dim_file_updated_at ON public.dim_file;

-- Drop the function
DROP FUNCTION IF EXISTS public.set_updated_at_timestamp();

COMMIT;
