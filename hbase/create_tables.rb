# HBase serving tables (PLAN.md §4.4).   pulse hbase-init    (idempotent: existing tables are kept)
# 16 regions pre-split on the first hex digit of the MD5 salt, so bulk loads and Storm writes spread
# across regions from the first row instead of all landing in one region that later splits.
splits = %w[1 2 3 4 5 6 7 8 9 a b c d e f]
tables = {
  'repo_hourly'  => 'c',   # per-repo hourly counts by event type
  'repo_similar' => 's',   # Mahout similar repos (+ m:name)
  'user_recs'    => 'r',   # Mahout "repos you may like"
  'repo_index'   => 'i',   # full_name -> repo_id
}
tables.each do |t, cf|
  next if list.include?(t)
  families = [{ NAME => cf, VERSIONS => 1, COMPRESSION => 'SNAPPY' }]
  families << { NAME => 'm', VERSIONS => 1 } if t == 'repo_similar'
  create t, *families, { SPLITS => splits }
end
# Written live by Storm. live_minute keys are time-ordered, so they get a (minute % 8) bucket prefix
# instead of an MD5 salt: consecutive minutes land in different regions, readers scan 8 small ranges.
create 'live_minute', { NAME => 't', VERSIONS => 1 }, { NAME => 'b', VERSIONS => 1 }, { SPLITS => %w[1 2 3 4 5 6 7] } unless list.include?('live_minute')
create 'live_topk', { NAME => 'k', VERSIONS => 1, TTL => 86400 } unless list.include?('live_topk')
create 'spike_alerts', { NAME => 'a', VERSIONS => 1, TTL => 7 * 86400 } unless list.include?('spike_alerts')
list
exit
