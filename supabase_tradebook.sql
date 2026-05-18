-- Run in Supabase SQL editor (project: ktech)

create table if not exists public.tradebook (
  id            bigserial primary key,
  symbol        text not null,
  entry_date    date not null,
  entry_price   numeric not null,
  entry_at      timestamptz,
  source_job_id bigint,
  current_price numeric,
  pnl_pct       numeric,
  updated_at    timestamptz default now()
);

create index if not exists tradebook_symbol_entry_date_idx
  on public.tradebook (symbol, entry_date desc);

alter table public.tradebook enable row level security;
