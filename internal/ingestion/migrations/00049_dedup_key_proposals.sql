-- +goose Up
-- +goose StatementBegin
CREATE TABLE dedup_key_proposals (
    id BIGSERIAL PRIMARY KEY,
    region_id UUID NOT NULL,
    source_id UUID NOT NULL,
    source_epoch BIGINT NOT NULL,
    seq_start BIGINT NOT NULL,
    seq_end BIGINT NOT NULL,
    factor_u UUID NOT NULL,
    factor_d UUID,
    dedup_key TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confirmed_at TIMESTAMPTZ,
    UNIQUE (region_id, source_id, source_epoch, seq_start, seq_end)
);

CREATE INDEX idx_dedup_proposals_pending_ttl
    ON dedup_key_proposals (created_at)
    WHERE status = 'pending';

CREATE OR REPLACE FUNCTION dedup_format_key(
    p_region_id UUID,
    p_source_id UUID,
    p_source_epoch BIGINT,
    p_seq_start BIGINT,
    p_seq_end BIGINT,
    p_factor_u UUID,
    p_factor_d UUID
) RETURNS TEXT AS $$
BEGIN
    RETURN format(
        'v2|%s|%s|%s|%s|%s|%s|%s',
        p_region_id,
        p_source_id,
        p_source_epoch,
        p_seq_start,
        p_seq_end,
        p_factor_u,
        p_factor_d
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION dedup_claim_confirm(
    p_region_id UUID,
    p_source_id UUID,
    p_source_epoch BIGINT,
    p_seq_start BIGINT,
    p_seq_end BIGINT,
    p_factor_u UUID
) RETURNS TABLE (
    outcome TEXT,
    factor_d UUID,
    dedup_key TEXT
) AS $$
DECLARE
    v_row dedup_key_proposals%ROWTYPE;
    v_new_d UUID;
    v_key TEXT;
BEGIN
    SELECT * INTO v_row
    FROM dedup_key_proposals
    WHERE region_id = p_region_id
      AND source_id = p_source_id
      AND source_epoch = p_source_epoch
      AND seq_start = p_seq_start
      AND seq_end = p_seq_end
    FOR UPDATE;

    IF FOUND THEN
        IF v_row.status = 'rejected' THEN
            RETURN QUERY SELECT 'rejected'::TEXT, NULL::UUID, v_row.dedup_key;
            RETURN;
        END IF;

        IF v_row.status = 'confirmed' THEN
            IF v_row.factor_u = p_factor_u THEN
                RETURN QUERY SELECT 'already_confirmed'::TEXT, v_row.factor_d, v_row.dedup_key;
            ELSE
                RETURN QUERY SELECT 'hash_mismatch'::TEXT, NULL::UUID, v_row.dedup_key;
            END IF;
            RETURN;
        END IF;

        IF v_row.status = 'pending' AND v_row.created_at < NOW() - INTERVAL '24 hours' THEN
            UPDATE dedup_key_proposals SET status = 'rejected' WHERE id = v_row.id;
            RETURN QUERY SELECT 'rejected'::TEXT, NULL::UUID, v_row.dedup_key;
            RETURN;
        END IF;

        IF v_row.factor_u = p_factor_u THEN
            v_new_d := gen_random_uuid();
            v_key := dedup_format_key(
                p_region_id, p_source_id, p_source_epoch, p_seq_start, p_seq_end, p_factor_u, v_new_d
            );
            UPDATE dedup_key_proposals
            SET status = 'confirmed',
                factor_d = v_new_d,
                dedup_key = v_key,
                confirmed_at = NOW()
            WHERE id = v_row.id;
            RETURN QUERY SELECT 'confirmed'::TEXT, v_new_d, v_key;
            RETURN;
        END IF;

        UPDATE dedup_key_proposals SET status = 'rejected' WHERE id = v_row.id;
        RETURN QUERY SELECT 'hash_mismatch'::TEXT, NULL::UUID, v_row.dedup_key;
        RETURN;
    END IF;

    v_new_d := gen_random_uuid();
    v_key := dedup_format_key(
        p_region_id, p_source_id, p_source_epoch, p_seq_start, p_seq_end, p_factor_u, v_new_d
    );
    INSERT INTO dedup_key_proposals (
        region_id, source_id, source_epoch, seq_start, seq_end,
        factor_u, factor_d, dedup_key, status, confirmed_at
    ) VALUES (
        p_region_id, p_source_id, p_source_epoch, p_seq_start, p_seq_end,
        p_factor_u, v_new_d, v_key, 'confirmed', NOW()
    );
    RETURN QUERY SELECT 'confirmed'::TEXT, v_new_d, v_key;
EXCEPTION
    WHEN unique_violation THEN
        RETURN QUERY
        SELECT * FROM dedup_claim_confirm(
            p_region_id, p_source_id, p_source_epoch, p_seq_start, p_seq_end, p_factor_u
        );
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP FUNCTION IF EXISTS dedup_claim_confirm(UUID, UUID, BIGINT, BIGINT, BIGINT, UUID);
DROP FUNCTION IF EXISTS dedup_format_key(UUID, UUID, BIGINT, BIGINT, BIGINT, UUID, UUID);
DROP TABLE IF EXISTS dedup_key_proposals;
-- +goose StatementEnd
