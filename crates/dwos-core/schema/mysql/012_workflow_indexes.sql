-- Separate from 007: an index in the same ALTER as the columns would rebuild the table.
ALTER TABLE workflow_status
    ADD KEY ix_workflow_status_parent (parent_workflow_id),
    ADD KEY ix_workflow_status_forked_from (forked_from);
