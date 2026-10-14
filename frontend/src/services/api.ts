import {
  CollaborationGraphData,
  CommitAuthor,
  CommitHistoryResponse,
  CommitItem,
  CommitTypeStat,
  ContributorStat,
  HdfsStatus,
  IngestionJob,
  RepoOverview,
  Repository,
  SparkStatus,
} from '../types'

// Use relative /api endpoint (proxied by Vite in dev) or fallback to absolute
const API_BASE = '/api'

export async function fetchHealth(): Promise<{ status: string }> {
  const res = await fetch(`${API_BASE}/health`)
  if (!res.ok) throw new Error('API server unreachable')
  return res.json()
}

export async function fetchRepositories(): Promise<Repository[]> {
  const res = await fetch(`${API_BASE}/repos`)
  if (!res.ok) throw new Error('Failed to fetch repositories')
  return res.json()
}

export async function fetchRepoOverview(owner: string, name: string): Promise<RepoOverview> {
  const res = await fetch(`${API_BASE}/repos/${owner}/${name}/overview`)
  if (!res.ok) throw new Error(`Failed to load overview for ${owner}/${name}`)
  return res.json()
}

export async function fetchCollaborationGraph(
  owner: string,
  name: string,
  minWeight: number = 2
): Promise<CollaborationGraphData> {
  const res = await fetch(
    `${API_BASE}/repos/${owner}/${name}/graph?min_weight=${minWeight}`
  )
  if (!res.ok) throw new Error(`Failed to load graph for ${owner}/${name}`)
  return res.json()
}

export async function fetchCommitTypes(owner: string, name: string): Promise<CommitTypeStat[]> {
  const res = await fetch(`${API_BASE}/repos/${owner}/${name}/commit-types`)
  if (!res.ok) throw new Error('Failed to load commit types')
  return res.json()
}

export async function fetchContributorStats(
  owner: string,
  name: string,
  limit: number = 100
): Promise<ContributorStat[]> {
  const res = await fetch(`${API_BASE}/repos/${owner}/${name}/stats?limit=${limit}`)
  if (!res.ok) throw new Error('Failed to load contributor stats')
  return res.json()
}

export async function triggerIngestion(payload: {
  repo_url: string
  access_token?: string
  branch?: string
}): Promise<{ job_id: string; owner: string; name: string; status: string; message: string }> {
  const res = await fetch(`${API_BASE}/repos/ingest`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: 'Failed to start ingestion' }))
    throw new Error(err.detail || 'Ingestion failed to start')
  }
  return res.json()
}

export async function fetchJobStatus(jobId: string): Promise<IngestionJob> {
  const res = await fetch(`${API_BASE}/repos/jobs/${jobId}`)
  if (!res.ok) throw new Error('Job not found')
  return res.json()
}

export async function fetchRecentJobs(): Promise<IngestionJob[]> {
  const res = await fetch(`${API_BASE}/repos/jobs?limit=15`)
  if (!res.ok) return []
  return res.json()
}

export async function triggerResync(owner: string, name: string): Promise<{ job_id: string }> {
  const res = await fetch(`${API_BASE}/repos/${owner}/${name}/sync`, { method: 'POST' })
  if (!res.ok) throw new Error('Failed to trigger re-sync')
  return res.json()
}

export async function deleteRepository(owner: string, name: string): Promise<void> {
  const res = await fetch(`${API_BASE}/repos/${owner}/${name}`, { method: 'DELETE' })
  if (!res.ok) throw new Error('Failed to delete repository')
}

export async function fetchHdfsStatus(): Promise<HdfsStatus> {
  const res = await fetch(`${API_BASE}/hdfs/status`)
  if (!res.ok) throw new Error('Failed to fetch HDFS status')
  return res.json()
}

export async function fetchSparkStatus(): Promise<SparkStatus> {
  const res = await fetch(`${API_BASE}/spark/status`)
  if (!res.ok) throw new Error('Failed to fetch Spark status')
  return res.json()
}

export interface FetchCommitsParams {
  owner: string
  name: string
  page?: number
  limit?: number
  author?: string
  search?: string
  commit_type?: string
  sort?: string
}

export async function fetchCommits(params: FetchCommitsParams): Promise<CommitHistoryResponse> {
  const q = new URLSearchParams()
  if (params.page) q.set('page', String(params.page))
  if (params.limit) q.set('limit', String(params.limit))
  if (params.author && params.author.trim()) q.set('author', params.author.trim())
  if (params.search && params.search.trim()) q.set('search', params.search.trim())
  if (params.commit_type && params.commit_type !== 'ALL') q.set('commit_type', params.commit_type)
  if (params.sort) q.set('sort', params.sort)

  const res = await fetch(`${API_BASE}/repos/${params.owner}/${params.name}/commits?${q.toString()}`)
  if (!res.ok) throw new Error('Failed to fetch commits')
  return res.json()
}

export async function fetchCommitAuthors(
  owner: string,
  name: string,
  query?: string,
): Promise<CommitAuthor[]> {
  const q = new URLSearchParams()
  if (query && query.trim()) q.set('q', query.trim())
  q.set('limit', '50')

  const res = await fetch(`${API_BASE}/repos/${owner}/${name}/commit-authors?${q.toString()}`)
  if (!res.ok) return []
  return res.json()
}

