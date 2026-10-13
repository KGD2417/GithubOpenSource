-- Relational source for Sqoop (PLAN.md §4.2). Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS repos (
  repo_id           BIGINT PRIMARY KEY,
  full_name         TEXT UNIQUE NOT NULL,
  owner_login       TEXT,
  owner_type        TEXT CHECK (owner_type IN ('User', 'Organization')),
  primary_language  TEXT,
  topics            TEXT,            -- comma-joined: Postgres arrays don't import through Sqoop
  license_spdx      TEXT,
  created_at        TIMESTAMPTZ,
  stargazers_count  INT,
  forks_count       INT,
  open_issues_count INT,
  is_fork           BOOLEAN,
  archived          BOOLEAN,
  api_status        SMALLINT,        -- 200 = found, 404 = deleted/private (kept so we don't refetch)
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()   -- Sqoop --check-column for lastmodified imports
);

CREATE TABLE IF NOT EXISTS known_bots (
  login    TEXT PRIMARY KEY,
  source   TEXT NOT NULL,            -- curated | suffix | api
  added_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS event_types (
  event_type         TEXT PRIMARY KEY,
  category           TEXT NOT NULL,
  description        TEXT NOT NULL,
  key_payload_fields TEXT
);

INSERT INTO event_types VALUES
 ('PushEvent',                     'code',          'Commits pushed to a branch',             'ref,size,distinct_size'),
 ('CreateEvent',                   'code',          'Repository, branch or tag created',      'ref_type,ref,description'),
 ('DeleteEvent',                   'code',          'Branch or tag deleted',                  'ref_type,ref'),
 ('PullRequestEvent',              'collaboration', 'PR opened, closed, merged, reopened',    'action,pull_request.merged,merged_at,created_at'),
 ('PullRequestReviewEvent',        'collaboration', 'PR review submitted',                    'review.state'),
 ('PullRequestReviewCommentEvent', 'collaboration', 'Comment on a PR diff',                   'action'),
 ('IssuesEvent',                   'collaboration', 'Issue opened, closed, reopened',         'action,issue.created_at,closed_at'),
 ('IssueCommentEvent',             'collaboration', 'Comment on an issue or PR',              'action,issue.number'),
 ('CommitCommentEvent',            'collaboration', 'Comment on a commit',                    'comment'),
 ('WatchEvent',                    'popularity',    'Repository starred',                     'action=started'),
 ('ForkEvent',                     'popularity',    'Repository forked',                      'forkee.id,forkee.full_name'),
 ('ReleaseEvent',                  'release',       'Release published',                      'release.tag_name,prerelease'),
 ('MemberEvent',                   'community',     'Collaborator added',                     'member.login'),
 ('PublicEvent',                   'community',     'Private repository made public',         ''),
 ('GollumEvent',                   'docs',          'Wiki page created or updated',           'pages'),
 ('DiscussionEvent',               'community',     'Discussion created',                     'discussion')
ON CONFLICT (event_type) DO NOTHING;

-- Sqoop export targets (written from Hive curated tables) and their staging twins
CREATE TABLE IF NOT EXISTS trending_daily (
  dt DATE, event_type TEXT, rank INT, repo_id BIGINT, repo_name TEXT,
  events BIGINT, prev_day_events BIGINT, growth_pct NUMERIC(10,2),
  PRIMARY KEY (dt, event_type, rank));
CREATE TABLE IF NOT EXISTS language_trends (
  dt DATE, language TEXT, events BIGINT, stars BIGINT, active_repos INT,
  PRIMARY KEY (dt, language));
CREATE TABLE IF NOT EXISTS trending_daily_stage  (LIKE trending_daily);
CREATE TABLE IF NOT EXISTS language_trends_stage (LIKE language_trends);
