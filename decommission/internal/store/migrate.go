package store

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5"
)

// ApplyMigrationFile executes a migration SQL file. The shipped migration is a
// single DDL script (multi-statement), which pgx runs in one simple protocol
// call when the statements contain no parameter placeholders.
func ApplyMigrationFile(ctx context.Context, pg *Postgres, path string) error {
	sqlBytes, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return applySQL(ctx, pg, string(sqlBytes))
}

func applySQL(ctx context.Context, pg *Postgres, sqlText string) error {
	_, err := pg.pool.Exec(ctx, sqlText)
	return err
}

// compile-time guard: pgx is used indirectly by the pool; keep the import for
// future per-statement migrations that need pgx.ErrNoRows handling.
var _ = pgx.ErrNoRows
