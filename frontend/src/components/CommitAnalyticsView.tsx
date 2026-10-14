import React, { useCallback, useEffect, useRef, useState } from 'react'
import {
  Calendar,
  Check,
  ChevronDown,
  Clock,
  Copy,
  ExternalLink,
  Filter,
  GitCommit,
  Loader2,
  RefreshCw,
  Search,
  User,
  X,
} from 'lucide-react'
import { fetchCommitAuthors, fetchCommits } from '../services/api'
import type { CommitAuthor, CommitItem, CommitTypeStat } from '../types'

interface CommitAnalyticsViewProps {
  repoOwner?: string
  repoName?: string
  commitTypes: CommitTypeStat[]
}

const TYPE_PALETTE: Record<string, { label: string; color: string; bg: string }> = {
  BUG_FIX: { label: 'Bug Fixes', color: '#ef4444', bg: 'rgba(239, 68, 68, 0.12)' },
  FEATURE: { label: 'Features', color: '#10b981', bg: 'rgba(16, 185, 129, 0.12)' },
  REFACTOR: { label: 'Refactoring', color: '#3b82f6', bg: 'rgba(59, 130, 246, 0.12)' },
  DOCUMENTATION: { label: 'Docs', color: '#94a3b8', bg: 'rgba(148, 163, 184, 0.12)' },
  PERFORMANCE: { label: 'Performance', color: '#f59e0b', bg: 'rgba(245, 158, 11, 0.12)' },
  TEST: { label: 'Tests', color: '#a855f7', bg: 'rgba(168, 85, 247, 0.12)' },
  SECURITY: { label: 'Security', color: '#ec4899', bg: 'rgba(236, 72, 153, 0.12)' },
  DEVOPS: { label: 'CI / DevOps', color: '#14b8a6', bg: 'rgba(20, 184, 166, 0.12)' },
  OTHER: { label: 'Other', color: '#71717a', bg: 'rgba(113, 113, 122, 0.12)' },
}

function formatRelativeTime(dateStr: string | null): string {
  if (!dateStr) return '—'
  const date = new Date(dateStr)
  if (isNaN(date.getTime())) return '—'

  const now = new Date()
  const diffSec = Math.floor((now.getTime() - date.getTime()) / 1000)

  if (diffSec < 60) return 'just now'
  const diffMin = Math.floor(diffSec / 60)
  if (diffMin < 60) return `${diffMin}m ago`
  const diffHours = Math.floor(diffMin / 60)
  if (diffHours < 24) return `${diffHours}h ago`
  const diffDays = Math.floor(diffHours / 24)
  if (diffDays < 30) return `${diffDays}d ago`
  const diffMonths = Math.floor(diffDays / 30)
  if (diffMonths < 12) return `${diffMonths}mo ago`
  const diffYears = Math.floor(diffDays / 365)
  return `${diffYears}y ago`
}

function formatFullDate(dateStr: string | null): string {
  if (!dateStr) return '—'
  const date = new Date(dateStr)
  if (isNaN(date.getTime())) return dateStr
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export const CommitAnalyticsView: React.FC<CommitAnalyticsViewProps> = ({
  repoOwner,
  repoName,
  commitTypes,
}) => {
  // Commit list state & pagination
  const [commits, setCommits] = useState<CommitItem[]>([])
  const [page, setPage] = useState<number>(1)
  const [totalCommits, setTotalCommits] = useState<number>(0)
  const [hasMore, setHasMore] = useState<boolean>(true)
  const [isLoading, setIsLoading] = useState<boolean>(false)
  const [isLoadingMore, setIsLoadingMore] = useState<boolean>(false)

  // Filters
  const [selectedType, setSelectedType] = useState<string | null>(null)
  const [authorQuery, setAuthorQuery] = useState<string>('')
  const [selectedAuthor, setSelectedAuthor] = useState<string | null>(null)
  const [searchQuery, setSearchQuery] = useState<string>('')
  const [sortOrder, setSortOrder] = useState<'desc' | 'asc'>('desc')

  // Author autocomplete
  const [authorSuggestions, setAuthorSuggestions] = useState<CommitAuthor[]>([])
  const [isAuthorDropdownOpen, setIsAuthorDropdownOpen] = useState<boolean>(false)
  const authorDropdownRef = useRef<HTMLDivElement>(null)

  // Copy hash indicator
  const [copiedHash, setCopiedHash] = useState<string | null>(null)

  // Intersection observer sentinel ref for automatic lazy loading
  const sentinelRef = useRef<HTMLDivElement | null>(null)

  const totalCategorized = commitTypes.reduce((acc, curr) => acc + curr.total, 0) || 1

  // ── Fetch Commits (Page 1 or Filter Change) ──────────────────────────────
  const loadInitialCommits = useCallback(async () => {
    if (!repoOwner || !repoName) return

    setIsLoading(true)
    try {
      const res = await fetchCommits({
        owner: repoOwner,
        name: repoName,
        page: 1,
        limit: 50,
        author: selectedAuthor || undefined,
        search: searchQuery || undefined,
        commit_type: selectedType || undefined,
        sort: sortOrder,
      })

      setCommits(res.commits)
      setTotalCommits(res.total)
      setHasMore(res.has_more)
      setPage(1)
    } catch (err) {
      console.error('Failed to load commits:', err)
    } finally {
      setIsLoading(false)
    }
  }, [repoOwner, repoName, selectedAuthor, searchQuery, selectedType, sortOrder])

  // Trigger initial fetch and reload on filter updates
  useEffect(() => {
    loadInitialCommits()
  }, [loadInitialCommits])

  // ── Load More Commits (Lazy Loading) ────────────────────────────────────
  const loadMoreCommits = useCallback(async () => {
    if (!repoOwner || !repoName || isLoadingMore || !hasMore || isLoading) return

    setIsLoadingMore(true)
    const nextPage = page + 1
    try {
      const res = await fetchCommits({
        owner: repoOwner,
        name: repoName,
        page: nextPage,
        limit: 50,
        author: selectedAuthor || undefined,
        search: searchQuery || undefined,
        commit_type: selectedType || undefined,
        sort: sortOrder,
      })

      setCommits((prev) => [...prev, ...res.commits])
      setTotalCommits(res.total)
      setHasMore(res.has_more)
      setPage(nextPage)
    } catch (err) {
      console.error('Failed to load more commits:', err)
    } finally {
      setIsLoadingMore(false)
    }
  }, [repoOwner, repoName, page, isLoadingMore, hasMore, isLoading, selectedAuthor, searchQuery, selectedType, sortOrder])

  // Automatic infinite scroll observer
  useEffect(() => {
    const sentinel = sentinelRef.current
    if (!sentinel) return

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && hasMore && !isLoading && !isLoadingMore) {
          loadMoreCommits()
        }
      },
      { rootMargin: '300px' }
    )

    observer.observe(sentinel)
    return () => observer.disconnect()
  }, [loadMoreCommits, hasMore, isLoading, isLoadingMore])

  // ── Author Search Autocomplete ──────────────────────────────────────────
  useEffect(() => {
    if (!repoOwner || !repoName) return

    const timer = setTimeout(async () => {
      try {
        const authors = await fetchCommitAuthors(repoOwner, repoName, authorQuery)
        setAuthorSuggestions(authors)
      } catch (err) {
        console.error('Failed to fetch author suggestions:', err)
      }
    }, 250)

    return () => clearTimeout(timer)
  }, [repoOwner, repoName, authorQuery])

  // Close author dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        authorDropdownRef.current &&
        !authorDropdownRef.current.contains(event.target as Node)
      ) {
        setIsAuthorDropdownOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const handleCopyHash = (hash: string) => {
    navigator.clipboard.writeText(hash)
    setCopiedHash(hash)
    setTimeout(() => setCopiedHash(null), 2000)
  }

  const handleSelectAuthor = (name: string) => {
    setSelectedAuthor(name)
    setAuthorQuery(name)
    setIsAuthorDropdownOpen(false)
  }

  const handleClearAuthor = () => {
    setSelectedAuthor(null)
    setAuthorQuery('')
  }

  const handleClearAllFilters = () => {
    setSelectedAuthor(null)
    setAuthorQuery('')
    setSelectedType(null)
    setSearchQuery('')
  }

  const hasActiveFilters = Boolean(selectedAuthor || selectedType || searchQuery)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* 1. Commit Intent Classification Header */}
      <div className="panel" style={{ padding: '20px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '8px' }}>
          <div>
            <div style={{ fontSize: '0.95rem', fontWeight: 600, color: 'var(--text-primary)' }}>
              Commit Taxonomy & Intent Classification
            </div>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
              Click any category below to filter the entire commit history in real time.
            </div>
          </div>
          {selectedType && (
            <button
              onClick={() => setSelectedType(null)}
              className="btn btn-secondary"
              style={{ padding: '4px 8px', fontSize: '0.75rem', display: 'flex', alignItems: 'center', gap: '5px' }}
            >
              <X size={12} /> Clear Taxonomy Filter
            </button>
          )}
        </div>

        {/* Segmented Distribution Bar */}
        <div
          style={{
            display: 'flex',
            height: '10px',
            borderRadius: '5px',
            overflow: 'hidden',
            marginBottom: '16px',
            background: '#1c1c1f',
          }}
        >
          {commitTypes.map((t) => {
            const cfg = TYPE_PALETTE[t.commit_type] || TYPE_PALETTE.OTHER
            const pct = (t.total / totalCategorized) * 100
            if (pct < 0.3) return null
            const isSelected = selectedType === t.commit_type
            return (
              <div
                key={t.commit_type}
                title={`${cfg.label}: ${t.total.toLocaleString()} commits (${pct.toFixed(1)}%) — Click to filter`}
                style={{
                  width: `${pct}%`,
                  backgroundColor: cfg.color,
                  cursor: 'pointer',
                  opacity: selectedType && !isSelected ? 0.25 : 1,
                  transform: isSelected ? 'scaleY(1.3)' : 'none',
                  transition: 'all 0.15s ease',
                }}
                onClick={() => setSelectedType(isSelected ? null : t.commit_type)}
              />
            )
          })}
        </div>

        {/* Category Filter Chips */}
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
          {commitTypes.map((t) => {
            const cfg = TYPE_PALETTE[t.commit_type] || TYPE_PALETTE.OTHER
            const pct = ((t.total / totalCategorized) * 100).toFixed(1)
            const isSelected = selectedType === t.commit_type

            return (
              <button
                key={t.commit_type}
                onClick={() => setSelectedType(isSelected ? null : t.commit_type)}
                style={{
                  background: isSelected ? '#27272a' : '#141416',
                  border: isSelected ? `1px solid ${cfg.color}` : '1px solid var(--border-subtle)',
                  boxShadow: isSelected ? `0 0 10px ${cfg.bg}` : 'none',
                  borderRadius: 'var(--radius-xs)',
                  padding: '6px 12px',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                  cursor: 'pointer',
                  color: isSelected ? '#fff' : 'var(--text-primary)',
                  fontSize: '0.775rem',
                  fontWeight: isSelected ? 600 : 400,
                  transition: 'all 0.15s ease',
                }}
              >
                <span style={{ width: '8px', height: '8px', borderRadius: '50%', backgroundColor: cfg.color }} />
                <span>{cfg.label}</span>
                <span className="mono" style={{ color: isSelected ? '#e4e4e7' : 'var(--text-muted)' }}>
                  {t.total.toLocaleString()} ({pct}%)
                </span>
              </button>
            )
          })}
        </div>
      </div>

      {/* 2. Entire Commit History Panel */}
      <div className="panel" style={{ padding: '20px 24px' }}>
        {/* Controls Header: Search person, search message, sort order */}
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            gap: '14px',
            marginBottom: '18px',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px' }}>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <GitCommit size={18} style={{ color: 'var(--text-muted)' }} />
                <span style={{ fontSize: '1rem', fontWeight: 600, color: 'var(--text-primary)' }}>
                  Full Commit History
                </span>
                <span
                  className="badge mono"
                  style={{
                    backgroundColor: 'rgba(255, 255, 255, 0.06)',
                    color: 'var(--text-secondary)',
                    padding: '2px 8px',
                    borderRadius: '12px',
                    fontSize: '0.75rem',
                  }}
                >
                  {totalCommits.toLocaleString()} commits
                </span>
              </div>
              <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '2px' }}>
                Querying the HDFS & Lakehouse repository history with lazy loading and instant person search.
              </div>
            </div>

            {/* Quick Refresh */}
            <button
              onClick={() => loadInitialCommits()}
              disabled={isLoading}
              className="btn btn-secondary"
              style={{ padding: '6px 12px', fontSize: '0.775rem', display: 'flex', alignItems: 'center', gap: '6px' }}
              title="Refresh commit stream"
            >
              <RefreshCw size={12} className={isLoading ? 'animate-spin' : ''} />
              <span>Refresh</span>
            </button>
          </div>

          {/* Search Bars & Filters Bar */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '12px',
              flexWrap: 'wrap',
              background: '#131316',
              padding: '12px 14px',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            {/* Person / Contributor Search Dropdown */}
            <div ref={authorDropdownRef} style={{ position: 'relative', flex: '1 1 240px', minWidth: '220px' }}>
              <div style={{ position: 'relative' }}>
                <User size={13} style={{ position: 'absolute', left: '10px', top: '10px', color: 'var(--text-muted)' }} />
                <input
                  type="text"
                  placeholder="Search every person (author/email)…"
                  className="form-input"
                  style={{
                    paddingLeft: '32px',
                    paddingRight: selectedAuthor ? '30px' : '10px',
                    fontSize: '0.8rem',
                    width: '100%',
                    background: selectedAuthor ? 'rgba(59, 130, 246, 0.08)' : undefined,
                    borderColor: selectedAuthor ? '#3b82f6' : undefined,
                  }}
                  value={authorQuery}
                  onChange={(e) => {
                    setAuthorQuery(e.target.value)
                    setIsAuthorDropdownOpen(true)
                  }}
                  onFocus={() => setIsAuthorDropdownOpen(true)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      setSelectedAuthor(authorQuery.trim() || null)
                      setIsAuthorDropdownOpen(false)
                    }
                  }}
                />
                {selectedAuthor ? (
                  <button
                    onClick={handleClearAuthor}
                    style={{
                      position: 'absolute',
                      right: '8px',
                      top: '8px',
                      background: 'none',
                      border: 'none',
                      color: 'var(--text-muted)',
                      cursor: 'pointer',
                      padding: '2px',
                    }}
                    title="Clear author"
                  >
                    <X size={13} />
                  </button>
                ) : (
                  <button
                    onClick={() => setIsAuthorDropdownOpen(!isAuthorDropdownOpen)}
                    style={{
                      position: 'absolute',
                      right: '8px',
                      top: '8px',
                      background: 'none',
                      border: 'none',
                      color: 'var(--text-muted)',
                      cursor: 'pointer',
                    }}
                  >
                    <ChevronDown size={13} />
                  </button>
                )}
              </div>

              {/* Autocomplete Dropdown */}
              {isAuthorDropdownOpen && authorSuggestions.length > 0 && (
                <div
                  style={{
                    position: 'absolute',
                    top: 'calc(100% + 4px)',
                    left: 0,
                    right: 0,
                    maxHeight: '260px',
                    overflowY: 'auto',
                    backgroundColor: '#18181b',
                    border: '1px solid var(--border-primary)',
                    borderRadius: 'var(--radius-sm)',
                    boxShadow: '0 8px 24px rgba(0,0,0,0.6)',
                    zIndex: 50,
                  }}
                >
                  <div style={{ padding: '6px 10px', fontSize: '0.7rem', color: 'var(--text-dim)', borderBottom: '1px solid var(--border-subtle)' }}>
                    Contributors ({authorSuggestions.length}) — Click to select
                  </div>
                  {authorSuggestions.map((a, idx) => (
                    <div
                      key={`${a.name}-${idx}`}
                      onClick={() => handleSelectAuthor(a.name)}
                      style={{
                        padding: '8px 12px',
                        cursor: 'pointer',
                        fontSize: '0.775rem',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        borderBottom: '1px solid rgba(255,255,255,0.03)',
                        transition: 'background 0.1s ease',
                      }}
                      onMouseEnter={(e) => (e.currentTarget.style.backgroundColor = '#27272a')}
                      onMouseLeave={(e) => (e.currentTarget.style.backgroundColor = 'transparent')}
                    >
                      <div>
                        <div style={{ fontWeight: 500, color: 'var(--text-primary)' }}>{a.name}</div>
                        {a.email && <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>{a.email}</div>}
                      </div>
                      <span className="mono" style={{ fontSize: '0.725rem', color: 'var(--text-dim)' }}>
                        {a.commit_count.toLocaleString()} commits
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Keyword / Hash Search */}
            <div style={{ position: 'relative', flex: '1 1 200px', minWidth: '180px' }}>
              <Search size={13} style={{ position: 'absolute', left: '10px', top: '10px', color: 'var(--text-muted)' }} />
              <input
                type="text"
                placeholder="Search message or hash…"
                className="form-input"
                style={{ paddingLeft: '32px', fontSize: '0.8rem', width: '100%' }}
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
              {searchQuery && (
                <button
                  onClick={() => setSearchQuery('')}
                  style={{
                    position: 'absolute',
                    right: '8px',
                    top: '8px',
                    background: 'none',
                    border: 'none',
                    color: 'var(--text-muted)',
                    cursor: 'pointer',
                  }}
                >
                  <X size={13} />
                </button>
              )}
            </div>

            {/* Sort Toggle */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Sort:</span>
              <button
                onClick={() => setSortOrder(sortOrder === 'desc' ? 'asc' : 'desc')}
                className="btn btn-secondary"
                style={{ padding: '6px 10px', fontSize: '0.75rem', display: 'flex', alignItems: 'center', gap: '4px' }}
              >
                <Clock size={12} />
                <span>{sortOrder === 'desc' ? 'Newest First' : 'Oldest First'}</span>
              </button>
            </div>

            {/* Clear All Filters */}
            {hasActiveFilters && (
              <button
                onClick={handleClearAllFilters}
                className="btn btn-secondary"
                style={{ padding: '6px 10px', fontSize: '0.75rem', color: 'var(--status-rose)', borderColor: 'rgba(239, 68, 68, 0.3)' }}
              >
                Reset Filters
              </button>
            )}
          </div>

          {/* Active Filter Indicators */}
          {hasActiveFilters && (
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
              <span style={{ fontSize: '0.725rem', color: 'var(--text-muted)' }}>Filtered by:</span>
              {selectedAuthor && (
                <span
                  style={{
                    backgroundColor: 'rgba(59, 130, 246, 0.15)',
                    color: '#60a5fa',
                    border: '1px solid rgba(59, 130, 246, 0.3)',
                    borderRadius: 'var(--radius-xs)',
                    padding: '2px 8px',
                    fontSize: '0.725rem',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '4px',
                  }}
                >
                  <User size={11} /> Person: {selectedAuthor}
                  <X size={11} style={{ cursor: 'pointer' }} onClick={handleClearAuthor} />
                </span>
              )}
              {selectedType && (
                <span
                  style={{
                    backgroundColor: TYPE_PALETTE[selectedType]?.bg || 'rgba(255,255,255,0.1)',
                    color: TYPE_PALETTE[selectedType]?.color || '#fff',
                    border: `1px solid ${TYPE_PALETTE[selectedType]?.color || '#555'}`,
                    borderRadius: 'var(--radius-xs)',
                    padding: '2px 8px',
                    fontSize: '0.725rem',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '4px',
                  }}
                >
                  Type: {TYPE_PALETTE[selectedType]?.label || selectedType}
                  <X size={11} style={{ cursor: 'pointer' }} onClick={() => setSelectedType(null)} />
                </span>
              )}
              {searchQuery && (
                <span
                  style={{
                    backgroundColor: 'rgba(255, 255, 255, 0.08)',
                    color: 'var(--text-secondary)',
                    border: '1px solid var(--border-primary)',
                    borderRadius: 'var(--radius-xs)',
                    padding: '2px 8px',
                    fontSize: '0.725rem',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '4px',
                  }}
                >
                  Search: "{searchQuery}"
                  <X size={11} style={{ cursor: 'pointer' }} onClick={() => setSearchQuery('')} />
                </span>
              )}
            </div>
          )}
        </div>

        {/* 3. The Commit Table */}
        <div
          style={{
            border: '1px solid var(--border-subtle)',
            borderRadius: 'var(--radius-sm)',
            overflow: 'hidden',
          }}
        >
          <div style={{ maxHeight: '600px', overflowY: 'auto' }}>
            <table className="data-table" style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr>
                  <th style={{ width: '110px' }}>Commit</th>
                  <th style={{ width: '120px' }}>Taxonomy</th>
                  <th>Message / Subject</th>
                  <th style={{ width: '220px' }}>Author / Person</th>
                  <th style={{ width: '130px' }}>Date</th>
                  <th style={{ width: '110px', textAlign: 'right' }}>Changes</th>
                </tr>
              </thead>
              <tbody>
                {commits.map((c) => {
                  const typeCfg = TYPE_PALETTE[c.commit_type] || TYPE_PALETTE.OTHER
                  const isCopied = copiedHash === c.hash

                  return (
                    <tr key={c.hash} style={{ transition: 'background 0.1s ease' }}>
                      {/* Commit Hash */}
                      <td>
                        <button
                          onClick={() => handleCopyHash(c.hash)}
                          className="mono"
                          style={{
                            background: 'rgba(255, 255, 255, 0.04)',
                            border: '1px solid var(--border-subtle)',
                            borderRadius: 'var(--radius-xs)',
                            padding: '3px 6px',
                            color: 'var(--text-secondary)',
                            fontSize: '0.725rem',
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '5px',
                            cursor: 'pointer',
                          }}
                          title="Click to copy full commit hash"
                        >
                          {isCopied ? <Check size={11} color="var(--status-emerald)" /> : <Copy size={11} />}
                          <span>{c.hash.substring(0, 7)}</span>
                        </button>
                      </td>

                      {/* Taxonomy Category */}
                      <td>
                        <span
                          style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '5px',
                            padding: '2px 8px',
                            borderRadius: '4px',
                            fontSize: '0.7rem',
                            fontWeight: 600,
                            backgroundColor: typeCfg.bg,
                            color: typeCfg.color,
                            border: `1px solid ${typeCfg.color}33`,
                            cursor: 'pointer',
                          }}
                          onClick={() => setSelectedType(c.commit_type)}
                          title={`Filter by ${typeCfg.label}`}
                        >
                          <span style={{ width: '5px', height: '5px', borderRadius: '50%', backgroundColor: typeCfg.color }} />
                          {typeCfg.label}
                        </span>
                      </td>

                      {/* Commit Subject */}
                      <td>
                        <div
                          style={{
                            fontWeight: 500,
                            color: 'var(--text-primary)',
                            fontSize: '0.8rem',
                            lineHeight: 1.4,
                            maxWidth: '520px',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                          }}
                          title={c.subject}
                        >
                          {c.subject || 'No commit message'}
                        </div>
                      </td>

                      {/* Author / Person */}
                      <td>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <div
                            style={{
                              width: '24px',
                              height: '24px',
                              borderRadius: '50%',
                              backgroundColor: '#27272a',
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'center',
                              fontSize: '0.65rem',
                              fontWeight: 600,
                              color: 'var(--text-secondary)',
                              flexShrink: 0,
                            }}
                          >
                            {(c.author_name || 'U').substring(0, 2).toUpperCase()}
                          </div>
                          <div style={{ minWidth: 0 }}>
                            <div
                              onClick={() => handleSelectAuthor(c.author_name)}
                              style={{
                                fontWeight: 500,
                                color: 'var(--text-primary)',
                                fontSize: '0.775rem',
                                cursor: 'pointer',
                                textDecoration: 'underline text-decoration-color: transparent',
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                              }}
                              title={`Filter by ${c.author_name}`}
                              onMouseEnter={(e) => (e.currentTarget.style.color = '#60a5fa')}
                              onMouseLeave={(e) => (e.currentTarget.style.color = 'var(--text-primary)')}
                            >
                              {c.author_name}
                            </div>
                            {c.author_email && (
                              <div
                                style={{
                                  fontSize: '0.675rem',
                                  color: 'var(--text-dim)',
                                  overflow: 'hidden',
                                  textOverflow: 'ellipsis',
                                  whiteSpace: 'nowrap',
                                }}
                              >
                                {c.author_email}
                              </div>
                            )}
                          </div>
                        </div>
                      </td>

                      {/* Date */}
                      <td>
                        <div
                          style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}
                          title={formatFullDate(c.author_date)}
                        >
                          {formatRelativeTime(c.author_date)}
                        </div>
                      </td>

                      {/* Impact / Diff stats */}
                      <td style={{ textAlign: 'right' }}>
                        <div style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: '0.725rem' }}>
                          {c.insertions > 0 && (
                            <span className="mono tabular" style={{ color: 'var(--status-emerald)' }}>
                              +{c.insertions.toLocaleString()}
                            </span>
                          )}
                          {c.deletions > 0 && (
                            <span className="mono tabular" style={{ color: 'var(--status-rose)' }}>
                              -{c.deletions.toLocaleString()}
                            </span>
                          )}
                          {c.insertions === 0 && c.deletions === 0 && (
                            <span className="mono tabular" style={{ color: 'var(--text-dim)' }}>
                              —
                            </span>
                          )}
                        </div>
                      </td>
                    </tr>
                  )
                })}

                {/* Empty State */}
                {!isLoading && commits.length === 0 && (
                  <tr>
                    <td colSpan={6} style={{ textAlign: 'center', padding: '48px 16px' }}>
                      <div style={{ color: 'var(--text-muted)', fontSize: '0.9rem', marginBottom: '8px' }}>
                        No commits match your current filter criteria.
                      </div>
                      {hasActiveFilters && (
                        <button
                          onClick={handleClearAllFilters}
                          className="btn btn-secondary"
                          style={{ fontSize: '0.75rem', marginTop: '4px' }}
                        >
                          Clear all filters
                        </button>
                      )}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>

            {/* Loading Indicator / Sentinel for Infinite Scroll */}
            <div
              ref={sentinelRef}
              style={{
                padding: '16px',
                textAlign: 'center',
                borderTop: '1px solid var(--border-subtle)',
                background: '#111113',
              }}
            >
              {isLoading && (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '8px', color: 'var(--text-muted)', fontSize: '0.8rem' }}>
                  <Loader2 size={15} className="animate-spin" />
                  <span>Loading commits from lakehouse...</span>
                </div>
              )}

              {isLoadingMore && (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '8px', color: 'var(--text-muted)', fontSize: '0.8rem' }}>
                  <Loader2 size={15} className="animate-spin" />
                  <span>Loading next batch of commits...</span>
                </div>
              )}

              {!isLoading && !isLoadingMore && hasMore && (
                <button
                  onClick={loadMoreCommits}
                  className="btn btn-secondary"
                  style={{ fontSize: '0.775rem', padding: '6px 14px' }}
                >
                  Load More Commits (Showing {commits.length.toLocaleString()} of {totalCommits.toLocaleString()})
                </button>
              )}

              {!isLoading && !hasMore && commits.length > 0 && (
                <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
                  ✓ All {totalCommits.toLocaleString()} commits loaded
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
