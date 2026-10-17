export interface Repository {
  id: number
  owner: string
  name: string
  full_name: string
  stars: number
  forks: number
  language: string | null
  default_branch: string
  head_commit_hash: string | null
  is_private: boolean
  synced_at: string
}

export interface HealthScore {
  contributor_diversity: number
  contributor_retention: number
  collaboration: number
  commit_consistency: number
  bus_factor_safety: number
  overall: number
  bus_factor_count: number
  top_contributor_pct: number
  computed_at: string
}

export interface RepoMetrics {
  total_contributors: number
  total_commits: number
  total_insertions: number
  total_deletions: number
  total_files_changed: number
  first_commit_period: string
  last_commit_period: string
}

export interface BusFactorModule {
  module_path: string
  bus_factor: number
  top_owner: string
  top_owner_pct: number
  computed_at?: string
}

export interface RepoOverview {
  repository: Repository
  health: HealthScore | null
  metrics: RepoMetrics
  critical_modules: BusFactorModule[]
}

export interface GraphNode {
  id: string
  degree?: number
  pagerank?: number
  commits?: number
  x?: number
  y?: number
  vx?: number
  vy?: number
}

export interface GraphEdge {
  source: string | GraphNode
  target: string | GraphNode
  weight: number
  type: string
}

export interface CollaborationGraphData {
  nodes: GraphNode[]
  edges: GraphEdge[]
}

export interface CommitTypeStat {
  commit_type: string
  total: number
}

export interface CommitItem {
  hash: string
  author_name: string
  author_email: string | null
  author_date: string | null
  subject: string
  commit_type: string
  insertions: number
  deletions: number
  files_changed: number
}

export interface CommitHistoryResponse {
  total: number
  page: number
  limit: number
  total_pages: number
  has_more: boolean
  commits: CommitItem[]
}

export interface CommitAuthor {
  name: string
  email: string | null
  commit_count: number
}

export interface ContributorStat {
  github_login: string
  email: string | null
  period: string
  commit_count: number
  insertions: number
  deletions: number
  files_changed: number
}

export interface IngestionJob {
  id: string
  repo_url: string
  repo_owner: string
  repo_name: string
  branch: string
  status:
    | 'queued'
    | 'cloning'
    | 'extracting'
    | 'hdfs_write'
    | 'analytics'
    | 'spark_etl'
    | 'completed'
    | 'failed'
  progress_pct: number
  current_step: string
  total_commits: number
  error_message: string | null
  started_at?: string
  completed_at?: string
  created_at: string
}

// ── Infrastructure Status Types ────────────────────────────────────────────

export interface HdfsParquetFile {
  path: string
  size_bytes: number
  size_mb: number
  modified: number
  replication: number
}

export interface HdfsStatus {
  namenode_url: string
  status: 'reachable' | 'unreachable'
  error?: string
  raw_parquet_files: HdfsParquetFile[]
  raw_total_files: number
  warehouse_namespaces: string[]
}

export interface SparkApp {
  id: string
  name: string
  status: string
  cores?: number
  memory_per_executor_mb?: number
  duration_ms?: number
  submit_date?: string
}

export interface SparkStatus {
  status: 'reachable' | 'unreachable'
  error?: string
  master_url?: string
  alive_workers?: number
  cores_used?: number
  cores_total?: number
  memory_used_mb?: number
  memory_total_mb?: number
  active_apps: SparkApp[]
  completed_apps: SparkApp[]
}
