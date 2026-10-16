/**
 * LiveEventStream.tsx
 * Real-time commit feed powered by SSE (GET /events/stream).
 * Allows users to choose and manage tracked repositories dynamically from the dashboard.
 */
import React, { useEffect, useMemo, useRef, useState } from 'react'
import {
  GitCommitHorizontal,
  Plus,
  Radio,
  Trash2,
  X,
  Zap,
} from 'lucide-react'
import {
  addStreamingRepo,
  fetchStreamingRepos,
  removeStreamingRepo,
} from '../services/api'
import type { Repository } from '../types'

// ── Types ──────────────────────────────────────────────────────────────────
interface LiveCommit {
  repo: string
  hash: string
  short_hash: string
  author: string
  message: string
  commit_type: string
  committed_at: string
}

interface LiveEventStreamProps {
  selectedRepo?: Repository | null
}

// ── Helpers ────────────────────────────────────────────────────────────────
const API_BASE = import.meta.env.VITE_API_BACKEND ?? 'http://localhost:8000'

const TYPE_COLORS: Record<string, string> = {
  FEATURE:       'var(--status-blue)',
  BUG_FIX:       'var(--status-rose)',
  REFACTOR:      'var(--status-amber)',
  DOCUMENTATION: '#a78bfa',
  PERFORMANCE:   '#34d399',
  TEST:          '#60a5fa',
  SECURITY:      '#f97316',
  DEVOPS:        '#94a3b8',
  OTHER:         'var(--text-dim)',
}

const typeColor = (t: string) => TYPE_COLORS[t] ?? TYPE_COLORS.OTHER

function timeAgo(iso: string): string {
  const secs = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (secs < 60) return `${secs}s ago`
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`
  return `${Math.floor(secs / 3600)}h ago`
}

// ── Commit Card ─────────────────────────────────────────────────────────────
const CommitCard: React.FC<{ commit: LiveCommit; isNew: boolean }> = ({ commit, isNew }) => (
  <div
    className={`live-commit-card${isNew ? ' live-commit-card--new' : ''}`}
    style={{
      display:        'flex',
      alignItems:     'flex-start',
      gap:            '10px',
      padding:        '10px 14px',
      borderRadius:   'var(--radius-sm)',
      background:     isNew
        ? 'rgba(16,185,129,0.05)'
        : 'var(--bg-subtle)',
      border:         `1px solid ${isNew ? 'rgba(16,185,129,0.2)' : 'var(--border-subtle)'}`,
      transition:     'background 1.4s ease, border-color 1.4s ease',
      animation:      isNew ? 'slideInCommit 0.3s ease-out' : 'none',
    }}
  >
    {/* Commit icon */}
    <div style={{ marginTop: 2, flexShrink: 0 }}>
      <GitCommitHorizontal size={14} color={typeColor(commit.commit_type)} />
    </div>

    {/* Main content */}
    <div style={{ flex: 1, minWidth: 0 }}>
      <div style={{
        display:        'flex',
        alignItems:     'center',
        gap:            '6px',
        flexWrap:       'wrap',
        marginBottom:   '3px',
      }}>
        {/* Repo badge */}
        <span style={{
          fontFamily:   'var(--font-mono)',
          fontSize:     '0.68rem',
          color:        'var(--text-dim)',
          flexShrink:   0,
        }}>
          {commit.repo}
        </span>

        {/* Commit type badge */}
        <span style={{
          fontSize:     '0.62rem',
          fontWeight:   600,
          color:        typeColor(commit.commit_type),
          background:   `${typeColor(commit.commit_type)}18`,
          padding:      '1px 5px',
          borderRadius: '3px',
          letterSpacing: '0.04em',
          flexShrink:   0,
        }}>
          {commit.commit_type}
        </span>
      </div>

      {/* Commit message */}
      <div style={{
        fontSize:     '0.8rem',
        color:        'var(--text-primary)',
        fontWeight:   500,
        overflow:     'hidden',
        textOverflow: 'ellipsis',
        whiteSpace:   'nowrap',
      }}>
        {commit.message || '(no message)'}
      </div>

      {/* Footer */}
      <div style={{
        marginTop:  '4px',
        display:    'flex',
        alignItems: 'center',
        gap:        '6px',
        fontSize:   '0.7rem',
        color:      'var(--text-dim)',
      }}>
        <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>
          {commit.short_hash}
        </span>
        <span>·</span>
        <span>{commit.author}</span>
        <span>·</span>
        <span>{timeAgo(commit.committed_at)}</span>
      </div>
    </div>
  </div>
)

// ── Main Component ──────────────────────────────────────────────────────────
export const LiveEventStream: React.FC<LiveEventStreamProps> = ({ selectedRepo }) => {
  const [commits, setCommits]             = useState<LiveCommit[]>([])
  const [newIds, setNewIds]               = useState<Set<string>>(new Set())
  const [connected, setConnected]         = useState(false)
  const [eventCount, setEventCount]       = useState(0)

  // Tracked repositories state
  const [trackedRepos, setTrackedRepos]   = useState<string[]>([])
  const [newRepoInput, setNewRepoInput]   = useState('')
  const [isUpdatingRepo, setIsUpdatingRepo] = useState(false)
  const [repoError, setRepoError]         = useState<string | null>(null)
  const [filterRepo, setFilterRepo]       = useState<string>('ALL')

  const sourceRef                         = useRef<EventSource | null>(null)
  const listRef                           = useRef<HTMLDivElement>(null)

  // 1. Fetch currently tracked repos from API
  const refreshTrackedRepos = async () => {
    try {
      const repos = await fetchStreamingRepos()
      setTrackedRepos(repos)
    } catch {
      // API might be warming up
    }
  }

  useEffect(() => {
    refreshTrackedRepos()
  }, [])

  // 2. SSE Stream setup
  useEffect(() => {
    const url = `${API_BASE}/events/stream`
    const src = new EventSource(url)
    sourceRef.current = src

    src.onopen = () => setConnected(true)

    src.onmessage = (e: MessageEvent) => {
      try {
        const commit: LiveCommit = JSON.parse(e.data)
        const key = `${commit.repo}:${commit.hash}`

        setEventCount(c => c + 1)
        setCommits(prev => [commit, ...prev].slice(0, 80))

        setNewIds(prev => new Set([...prev, key]))
        setTimeout(() => {
          setNewIds(prev => {
            const next = new Set(prev)
            next.delete(key)
            return next
          })
        }, 3000)

        listRef.current?.scrollTo({ top: 0, behavior: 'smooth' })
      } catch {
        // malformed event — ignore
      }
    }

    src.onerror = () => {
      setConnected(false)
    }

    return () => {
      src.close()
      sourceRef.current = null
    }
  }, [])

  // 3. Handlers for adding/removing tracked repos
  const handleAddRepo = async (repoName: string) => {
    const target = repoName.trim()
    if (!target) return
    setIsUpdatingRepo(true)
    setRepoError(null)
    try {
      const res = await addStreamingRepo(target)
      setTrackedRepos(res.tracked_repos)
      setNewRepoInput('')
    } catch (err: any) {
      setRepoError(err.message || 'Failed to add repository')
    } finally {
      setIsUpdatingRepo(false)
    }
  }

  const handleRemoveRepo = async (repoName: string) => {
    if (trackedRepos.length <= 1) {
      setRepoError('At least one repository must remain in the stream.')
      setTimeout(() => setRepoError(null), 3000)
      return
    }
    setIsUpdatingRepo(true)
    setRepoError(null)
    try {
      const res = await removeStreamingRepo(repoName)
      setTrackedRepos(res.tracked_repos)
      if (filterRepo === repoName) {
        setFilterRepo('ALL')
      }
    } catch (err: any) {
      setRepoError(err.message || 'Failed to remove repository')
    } finally {
      setIsUpdatingRepo(false)
    }
  }

  // Filtered commits
  const displayedCommits = useMemo(() => {
    if (filterRepo === 'ALL') return commits
    return commits.filter(c => c.repo === filterRepo)
  }, [commits, filterRepo])

  const selectedRepoFullName = selectedRepo?.full_name ?? (selectedRepo ? `${selectedRepo.owner}/${selectedRepo.name}` : null)
  const isSelectedRepoTracked = selectedRepoFullName ? trackedRepos.includes(selectedRepoFullName) : false

  return (
    <section
      style={{
        display:       'flex',
        flexDirection: 'column',
        gap:           '0',
        height:        '100%',
      }}
    >
      {/* ── Top Header ──────────────────────────────────────────────────────── */}
      <div style={{
        display:        'flex',
        alignItems:     'center',
        justifyContent: 'space-between',
        padding:        '12px 16px 10px',
        borderBottom:   '1px solid var(--border-subtle)',
        flexShrink:     0,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Zap size={15} color="var(--status-emerald)" />
          <span style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--text-primary)' }}>
            Live Commits Feed
          </span>
          <span style={{
            fontSize:     '0.68rem',
            color:        'var(--text-dim)',
            background:   'var(--bg-subtle)',
            padding:      '1px 6px',
            borderRadius: '10px',
            border:       '1px solid var(--border-subtle)',
          }}>
            Kafka + SSE
          </span>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          {/* Total events counter */}
          {eventCount > 0 && (
            <span style={{
              fontSize:           '0.7rem',
              color:              'var(--text-dim)',
              fontVariantNumeric: 'tabular-nums',
            }}>
              {eventCount} events
            </span>
          )}

          {/* Clear button */}
          {commits.length > 0 && (
            <button
              onClick={() => setCommits([])}
              title="Clear current stream"
              style={{
                display:         'flex',
                alignItems:      'center',
                gap:             '4px',
                background:      'none',
                border:          'none',
                color:           'var(--text-muted)',
                cursor:          'pointer',
                fontSize:        '0.68rem',
                padding:         '2px 6px',
                borderRadius:    'var(--radius-sm)',
              }}
            >
              <Trash2 size={12} />
              <span>Clear</span>
            </button>
          )}

          {/* Connection status badge */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span
              style={{
                display:         'inline-block',
                width:           6,
                height:          6,
                borderRadius:    '50%',
                backgroundColor: connected ? 'var(--status-emerald)' : 'var(--status-amber)',
                boxShadow:       connected
                  ? '0 0 6px var(--status-emerald)'
                  : '0 0 6px var(--status-amber)',
                animation:       connected ? 'livePulse 2s infinite' : 'none',
              }}
            />
            <span style={{ fontSize: '0.68rem', color: 'var(--text-dim)', fontWeight: 500 }}>
              {connected ? 'LIVE' : 'Connecting…'}
            </span>
          </div>
        </div>
      </div>

      {/* ── Repository Management Bar ───────────────────────────────────────── */}
      <div style={{
        padding:        '10px 16px',
        background:     'var(--bg-subtle)',
        borderBottom:   '1px solid var(--border-subtle)',
        display:        'flex',
        flexDirection:  'column',
        gap:            '8px',
        flexShrink:     0,
      }}>
        {/* Row 1: Tracked Repos List */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '8px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap' }}>
            <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'flex', alignItems: 'center', gap: '4px' }}>
              <Radio size={12} color="var(--status-emerald)" />
              Streaming:
            </span>

            {trackedRepos.length === 0 ? (
              <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>Loading repos…</span>
            ) : (
              trackedRepos.map(repo => {
                const isActiveFilter = filterRepo === repo
                return (
                  <span
                    key={repo}
                    style={{
                      display:        'inline-flex',
                      alignItems:     'center',
                      gap:            '5px',
                      padding:        '2px 8px',
                      borderRadius:   '12px',
                      fontSize:       '0.72rem',
                      fontFamily:     'var(--font-mono)',
                      background:     isActiveFilter ? 'rgba(16,185,129,0.15)' : 'var(--bg-card)',
                      color:          isActiveFilter ? 'var(--status-emerald)' : 'var(--text-primary)',
                      border:         `1px solid ${isActiveFilter ? 'var(--status-emerald)' : 'var(--border-subtle)'}`,
                    }}
                  >
                    <span
                      onClick={() => setFilterRepo(prev => prev === repo ? 'ALL' : repo)}
                      title={`Filter stream to ${repo}`}
                      style={{ cursor: 'pointer' }}
                    >
                      {repo}
                    </span>
                    <button
                      onClick={() => handleRemoveRepo(repo)}
                      disabled={isUpdatingRepo}
                      title={`Stop streaming ${repo}`}
                      style={{
                        background: 'none',
                        border:     'none',
                        color:      'var(--text-dim)',
                        cursor:     'pointer',
                        padding:    0,
                        display:    'flex',
                        alignItems: 'center',
                      }}
                    >
                      <X size={11} />
                    </button>
                  </span>
                )
              })
            )}
          </div>

          {/* Quick track active dashboard repo if not already in list */}
          {selectedRepoFullName && !isSelectedRepoTracked && (
            <button
              onClick={() => handleAddRepo(selectedRepoFullName)}
              disabled={isUpdatingRepo}
              style={{
                display:        'inline-flex',
                alignItems:     'center',
                gap:            '5px',
                padding:        '3px 10px',
                borderRadius:   'var(--radius-sm)',
                fontSize:       '0.72rem',
                fontWeight:     500,
                background:     'rgba(59,130,246,0.12)',
                color:          'var(--status-blue)',
                border:         '1px solid rgba(59,130,246,0.3)',
                cursor:         'pointer',
                transition:     'background 0.2s',
              }}
            >
              <Plus size={12} />
              <span>+ Stream {selectedRepoFullName}</span>
            </button>
          )}
        </div>

        {/* Row 2: Add custom repo input & filter toggle */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '10px', flexWrap: 'wrap' }}>
          {/* Add repo input form */}
          <form
            onSubmit={(e) => {
              e.preventDefault()
              handleAddRepo(newRepoInput)
            }}
            style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <input
              type="text"
              placeholder="Add repo (owner/repo or github url)…"
              value={newRepoInput}
              onChange={(e) => setNewRepoInput(e.target.value)}
              disabled={isUpdatingRepo}
              style={{
                background:   'var(--bg-card)',
                border:       '1px solid var(--border-subtle)',
                borderRadius: 'var(--radius-sm)',
                padding:      '3px 8px',
                fontSize:     '0.72rem',
                color:        'var(--text-primary)',
                fontFamily:   'var(--font-mono)',
                width:        '240px',
                outline:      'none',
              }}
            />
            <button
              type="submit"
              disabled={isUpdatingRepo || !newRepoInput.trim()}
              style={{
                display:        'inline-flex',
                alignItems:     'center',
                gap:            '4px',
                padding:        '3px 8px',
                borderRadius:   'var(--radius-sm)',
                fontSize:       '0.72rem',
                background:     'var(--bg-card)',
                border:         '1px solid var(--border-subtle)',
                color:          newRepoInput.trim() ? 'var(--text-primary)' : 'var(--text-dim)',
                cursor:         newRepoInput.trim() ? 'pointer' : 'default',
              }}
            >
              <Plus size={12} />
              <span>Add</span>
            </button>
          </form>

          {/* Filter pills if multiple tracked repos */}
          {trackedRepos.length > 1 && (
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
              <span style={{ fontSize: '0.68rem', color: 'var(--text-dim)' }}>Filter:</span>
              <button
                onClick={() => setFilterRepo('ALL')}
                style={{
                  fontSize:     '0.68rem',
                  padding:      '2px 7px',
                  borderRadius: '4px',
                  background:   filterRepo === 'ALL' ? 'var(--border-subtle)' : 'transparent',
                  color:        filterRepo === 'ALL' ? 'var(--text-primary)' : 'var(--text-dim)',
                  border:       'none',
                  cursor:       'pointer',
                }}
              >
                All ({commits.length})
              </button>
              {trackedRepos.map(r => (
                <button
                  key={r}
                  onClick={() => setFilterRepo(r)}
                  style={{
                    fontSize:     '0.68rem',
                    padding:      '2px 7px',
                    borderRadius: '4px',
                    background:   filterRepo === r ? 'var(--border-subtle)' : 'transparent',
                    color:        filterRepo === r ? 'var(--text-primary)' : 'var(--text-dim)',
                    border:       'none',
                    cursor:       'pointer',
                    fontFamily:   'var(--font-mono)',
                  }}
                >
                  {r.split('/')[1] || r}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Error message */}
        {repoError && (
          <div style={{ fontSize: '0.7rem', color: 'var(--status-rose)', marginTop: '2px' }}>
            {repoError}
          </div>
        )}
      </div>

      {/* ── Commit List ─────────────────────────────────────────────────────── */}
      <div
        ref={listRef}
        style={{
          flex:      1,
          overflowY: 'auto',
          padding:   '10px 14px',
          display:   'flex',
          flexDirection: 'column',
          gap:       '6px',
        }}
      >
        {displayedCommits.length === 0 ? (
          <div style={{
            flex:           1,
            display:        'flex',
            flexDirection:  'column',
            alignItems:     'center',
            justifyContent: 'center',
            gap:            '8px',
            color:          'var(--text-dim)',
            fontSize:       '0.78rem',
            padding:        '32px 16px',
            textAlign:      'center',
          }}>
            <Zap size={22} color="var(--border-active)" />
            <span>
              {filterRepo !== 'ALL'
                ? `Waiting for live commits from ${filterRepo}…`
                : 'Waiting for live commits…'}
            </span>
            <span style={{ fontSize: '0.68rem', color: 'var(--text-dim)', maxWidth: '380px' }}>
              The Kafka producer polls GitHub Events every 30s. Push commits to tracked repositories or wait for upcoming activity.
            </span>
          </div>
        ) : (
          displayedCommits.map(c => {
            const key = `${c.repo}:${c.hash}`
            return (
              <CommitCard
                key={key}
                commit={c}
                isNew={newIds.has(key)}
              />
            )
          })
        )}
      </div>
    </section>
  )
}

export default LiveEventStream
