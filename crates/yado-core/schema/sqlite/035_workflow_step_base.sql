-- Steps up to this ID belong to runs that continued as new. The current run starts after it.
ALTER TABLE workflow_status ADD COLUMN step_base INTEGER NOT NULL DEFAULT 0;
