-- Deploy FileDimension:feature/auto_update_timestamp to pg

BEGIN;

-- 1. Create the trigger function
CREATE OR REPLACE FUNCTION public.set_updated_at_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  -- Set the updated_at column of the NEW row to the current transaction timestamp
  NEW.updated_at = NOW();
  -- Return the modified row to be inserted
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add a comment for the new function
COMMENT ON FUNCTION public.set_updated_at_timestamp()
  IS 'This trigger function automatically sets the updated_at column to the current timestamp.';

-- 2. Create the trigger and attach the function to the dim_file table
CREATE TRIGGER trigger_dim_file_updated_at
BEFORE UPDATE ON public.dim_file
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at_timestamp();

COMMIT;
