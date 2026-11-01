create table if not exists public.bot_state (
  id text primary key,
  data jsonb not null,
  updated_at timestamptz not null default now()
);

-- Доступ лише через service_role ключ (він обходить RLS); для anon/authenticated доступу немає
alter table public.bot_state enable row level security;
