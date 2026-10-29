-- BNVT Dealflow: Supabase schema. Run once in Supabase > SQL Editor > New query > Run.
-- Safe to run again (idempotent).

-- 1. Full-database snapshot (already used by the app)
create table if not exists public.key_value_store (
  key   text primary key,
  value text
);

-- 2. One row per record, so you can browse/query deals in Supabase and every record is verifiable
do $$
declare t text;
begin
  foreach t in array array['companies','investors','deep_dives','founder_profiles','reading_items','reports']
  loop
    execute format('create table if not exists public.%I (
        id          text primary key,
        data        jsonb not null,
        hash        text  not null,
        updated_at  timestamptz not null default now()
      )', t);
    execute format('create index if not exists %I on public.%I (updated_at desc)', t || '_updated_idx', t);
    -- Lock tables down: only the server (secret key) can read/write. No public access.
    execute format('alter table public.%I enable row level security', t);
  end loop;
end $$;

alter table public.key_value_store enable row level security;

-- 3. Private file bucket for uploaded articles, reports and decks
insert into storage.buckets (id, name, public)
values ('reading', 'reading', false)
on conflict (id) do nothing;
