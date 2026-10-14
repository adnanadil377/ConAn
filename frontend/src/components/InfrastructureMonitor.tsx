import React, { useEffect, useState } from 'react'
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Clock,
  Database,
  HardDrive,
  RefreshCw,
  Server,
  Zap,
} from 'lucide-react'
import { fetchHdfsStatus, fetchSparkStatus } from '../services/api'
import type { HdfsStatus, SparkStatus } from '../types'

interface Props {
  repoOwner?: string
  repoName?: string
}

function StatusDot({ ok }: { ok: boolean }) {
  return (
    <span
      style={{
        display: 'inline-block',
        width: 8,
        height: 8,
        borderRadius: '50%',
        backgroundColor: ok ? 'var(--status-emerald)' : 'var(--status-rose)',
        flexShrink: 0,
      }}
    />
  )
}

function MetricCell({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: 4,
        padding: '12px 14px',
        background: 'rgba(255,255,255,0.02)',
        borderRadius: 'var(--radius-sm)',
        border: '1px solid var(--border-subtle)',
      }}
    >
      <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
        {label}
      </span>
      <span style={{ fontSize: '1.05rem', fontWeight: 600, color: 'var(--text-primary)', fontVariantNumeric: 'tabular-nums' }}>
        {value}
      </span>
    </div>
  )
}

function SectionHeader({ icon, title, status }: { icon: React.ReactNode; title: string; status?: boolean }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14 }}>
      <div style={{ color: 'var(--text-secondary)' }}>{icon}</div>
      <span style={{ fontWeight: 600, fontSize: '0.9rem', color: 'var(--text-primary)' }}>{title}</span>
      {status !== undefined && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginLeft: 'auto' }}>
          <StatusDot ok={status} />
          <span style={{ fontSize: '0.72rem', color: status ? 'var(--status-emerald)' : 'var(--status-rose)' }}>
            {status ? 'Reachable' : 'Unreachable'}
          </span>
        </div>
      )}
    </div>
  )
}

function formatBytes(bytes: number) {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`
}

function formatMs(ms?: number) {
  if (!ms) return '—'
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${(ms / 60000).toFixed(1)}m`
}

function AppStatusBadge({ status }: { status: string }) {
  const colors: Record<string, { bg: string; text: string }> = {
    FINISHED: { bg: 'rgba(16,185,129,0.12)', text: '#10b981' },
    RUNNING:  { bg: 'rgba(59,130,246,0.12)', text: '#60a5fa' },
    FAILED:   { bg: 'rgba(239,68,68,0.12)',  text: '#f87171' },
    KILLED:   { bg: 'rgba(245,158,11,0.12)', text: '#fbbf24' },
  }
  const c = colors[status?.toUpperCase()] || { bg: 'rgba(100,100,100,0.12)', text: 'var(--text-muted)' }
  return (
    <span
      style={{
        padding: '2px 7px',
        borderRadius: 4,
        fontSize: '0.68rem',
        fontWeight: 600,
        background: c.bg,
        color: c.text,
        textTransform: 'uppercase',
        letterSpacing: '0.05em',
      }}
    >
      {status}
    </span>
  )
}

export const InfrastructureMonitor: React.FC<Props> = () => {
  const [hdfs, setHdfs]           = useState<HdfsStatus | null>(null)
  const [spark, setSpark]         = useState<SparkStatus | null>(null)
  const [loading, setLoading]     = useState(true)
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date())

  const refresh = async () => {
    setLoading(true)
    try {
      const [hdfsRes, sparkRes] = await Promise.all([fetchHdfsStatus(), fetchSparkStatus()])
      setHdfs(hdfsRes)
      setSpark(sparkRes)
      setLastRefresh(new Date())
    } catch (err) {
      console.error('Failed to load infra status', err)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    refresh()
    const interval = setInterval(refresh, 30000)  // auto-refresh every 30s
    return () => clearInterval(interval)
  }, [])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div>
          <h2 style={{ fontSize: '1rem', fontWeight: 600, color: 'var(--text-primary)', margin: 0 }}>
            Infrastructure Monitor
          </h2>
          <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)', marginTop: 3 }}>
            HDFS NameNode • Apache Spark • Iceberg Lakehouse — live cluster status
          </p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontSize: '0.72rem', color: 'var(--text-dim)' }}>
            Last refreshed: {lastRefresh.toLocaleTimeString()}
          </span>
          <button
            className="btn btn-secondary btn-sm"
            onClick={refresh}
            disabled={loading}
            title="Refresh status"
          >
            <RefreshCw size={12} style={{ animation: loading ? 'spin 1s linear infinite' : 'none' }} />
            <span>{loading ? 'Refreshing…' : 'Refresh'}</span>
          </button>
        </div>
      </div>

      {/* ── HDFS Panel ── */}
      <div className="panel" style={{ padding: '18px 20px' }}>
        <SectionHeader
          icon={<HardDrive size={16} />}
          title="HDFS — Hadoop Distributed File System"
          status={hdfs?.status === 'reachable'}
        />

        {hdfs?.status === 'unreachable' ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: 'var(--status-rose)', fontSize: '0.8rem' }}>
            <AlertTriangle size={14} />
            <span>HDFS NameNode unreachable: {hdfs.error}</span>
          </div>
        ) : (
          <>
            {/* Metrics Row */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: 10, marginBottom: 18 }}>
              <MetricCell
                label="Raw Parquet Files"
                value={hdfs?.raw_total_files ?? '—'}
              />
              <MetricCell
                label="Total Size"
                value={
                  hdfs?.raw_parquet_files
                    ? formatBytes(hdfs.raw_parquet_files.reduce((s, f) => s + f.size_bytes, 0))
                    : '—'
                }
              />
              <MetricCell
                label="Iceberg Namespaces"
                value={hdfs?.warehouse_namespaces?.length ?? '—'}
              />
              <MetricCell
                label="NameNode"
                value={
                  <span style={{ fontSize: '0.78rem' }}>
                    {hdfs?.namenode_url?.replace('http://', '') ?? '—'}
                  </span>
                }
              />
            </div>

            {/* Raw Files Table */}
            {hdfs?.raw_parquet_files && hdfs.raw_parquet_files.length > 0 && (
              <div>
                <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: 8 }}>
                  Raw Parquet Files (/raw/git_events/)
                </div>
                <div style={{ overflowX: 'auto' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
                    <thead>
                      <tr style={{ borderBottom: '1px solid var(--border-subtle)' }}>
                        {['HDFS Path', 'Size', 'Replication', 'Last Modified'].map(h => (
                          <th
                            key={h}
                            style={{
                              padding: '5px 10px 7px',
                              textAlign: 'left',
                              color: 'var(--text-muted)',
                              fontWeight: 500,
                              fontSize: '0.7rem',
                              letterSpacing: '0.05em',
                            }}
                          >
                            {h}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {hdfs.raw_parquet_files.map((f, i) => (
                        <tr
                          key={i}
                          style={{
                            borderBottom: '1px solid var(--border-subtle)',
                            transition: 'background 0.1s',
                          }}
                          onMouseEnter={e => (e.currentTarget.style.background = 'rgba(255,255,255,0.025)')}
                          onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                        >
                          <td style={{ padding: '7px 10px' }}>
                            <code style={{ fontSize: '0.73rem', color: 'var(--text-secondary)' }}>
                              hdfs://namenode:9000{f.path}
                            </code>
                          </td>
                          <td style={{ padding: '7px 10px', color: 'var(--text-primary)', fontVariantNumeric: 'tabular-nums' }}>
                            {formatBytes(f.size_bytes)}
                          </td>
                          <td style={{ padding: '7px 10px', color: 'var(--text-muted)' }}>
                            {f.replication}×
                          </td>
                          <td style={{ padding: '7px 10px', color: 'var(--text-muted)', fontVariantNumeric: 'tabular-nums' }}>
                            {new Date(f.modified).toLocaleString()}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Iceberg Warehouse Namespaces */}
            {hdfs?.warehouse_namespaces && hdfs.warehouse_namespaces.length > 0 && (
              <div style={{ marginTop: 16 }}>
                <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: 8 }}>
                  Iceberg Warehouse (/warehouse/data/)
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                  {hdfs.warehouse_namespaces.map((ns) => (
                    <span
                      key={ns}
                      style={{
                        padding: '3px 9px',
                        borderRadius: 4,
                        background: 'rgba(59,130,246,0.1)',
                        color: '#93c5fd',
                        fontSize: '0.73rem',
                        fontFamily: 'monospace',
                        border: '1px solid rgba(59,130,246,0.2)',
                      }}
                    >
                      {ns}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {(!hdfs?.raw_parquet_files || hdfs.raw_parquet_files.length === 0) && (
              <div
                style={{
                  padding: '24px 0',
                  textAlign: 'center',
                  color: 'var(--text-dim)',
                  fontSize: '0.82rem',
                }}
              >
                No Parquet files in HDFS yet — ingest a repository to populate the lakehouse.
              </div>
            )}
          </>
        )}
      </div>

      {/* ── Spark Panel ── */}
      <div className="panel" style={{ padding: '18px 20px' }}>
        <SectionHeader
          icon={<Zap size={16} />}
          title="Apache Spark — Cluster Status"
          status={spark?.status === 'reachable'}
        />

        {spark?.status === 'unreachable' ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: 'var(--status-rose)', fontSize: '0.8rem' }}>
            <AlertTriangle size={14} />
            <span>Spark master unreachable: {spark.error}</span>
          </div>
        ) : (
          <>
            {/* Cluster Metrics */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(130px, 1fr))', gap: 10, marginBottom: 18 }}>
              <MetricCell label="Alive Workers" value={spark?.alive_workers ?? '—'} />
              <MetricCell
                label="Cores Used"
                value={`${spark?.cores_used ?? 0} / ${spark?.cores_total ?? 0}`}
              />
              <MetricCell
                label="Memory Used"
                value={`${Math.round((spark?.memory_used_mb ?? 0) / 1024)}G / ${Math.round((spark?.memory_total_mb ?? 0) / 1024)}G`}
              />
              <MetricCell
                label="Active Jobs"
                value={spark?.active_apps?.length ?? 0}
              />
            </div>

            {/* Active Applications */}
            {spark?.active_apps && spark.active_apps.length > 0 && (
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--status-emerald)', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 5 }}>
                  <Activity size={12} />
                  Active Applications
                </div>
                {spark.active_apps.map((app) => (
                  <div
                    key={app.id}
                    style={{
                      padding: '10px 12px',
                      borderRadius: 'var(--radius-xs)',
                      background: 'rgba(16,185,129,0.06)',
                      border: '1px solid rgba(16,185,129,0.15)',
                      marginBottom: 8,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      gap: 10,
                    }}
                  >
                    <div>
                      <span style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: '0.82rem' }}>
                        {app.name}
                      </span>
                      <span style={{ marginLeft: 8, fontSize: '0.72rem', color: 'var(--text-dim)', fontFamily: 'monospace' }}>
                        {app.id}
                      </span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                      <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                        {app.cores} cores · {app.memory_per_executor_mb}MB/exec
                      </span>
                      <AppStatusBadge status={app.status} />
                    </div>
                  </div>
                ))}
              </div>
            )}

            {/* Completed Applications */}
            {spark?.completed_apps && spark.completed_apps.length > 0 && (
              <div>
                <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 5 }}>
                  <CheckCircle2 size={12} />
                  Completed Jobs (last 10)
                </div>
                <div style={{ overflowX: 'auto' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
                    <thead>
                      <tr style={{ borderBottom: '1px solid var(--border-subtle)' }}>
                        {['Job Name', 'App ID', 'Status', 'Duration', 'Submitted'].map(h => (
                          <th
                            key={h}
                            style={{
                              padding: '5px 10px 7px',
                              textAlign: 'left',
                              color: 'var(--text-muted)',
                              fontWeight: 500,
                              fontSize: '0.7rem',
                              letterSpacing: '0.05em',
                            }}
                          >
                            {h}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {spark.completed_apps.map((app) => (
                        <tr
                          key={app.id}
                          style={{ borderBottom: '1px solid var(--border-subtle)' }}
                          onMouseEnter={e => (e.currentTarget.style.background = 'rgba(255,255,255,0.025)')}
                          onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                        >
                          <td style={{ padding: '7px 10px', fontWeight: 500, color: 'var(--text-primary)' }}>
                            {app.name}
                          </td>
                          <td style={{ padding: '7px 10px' }}>
                            <code style={{ fontSize: '0.72rem', color: 'var(--text-dim)' }}>{app.id}</code>
                          </td>
                          <td style={{ padding: '7px 10px' }}>
                            <AppStatusBadge status={app.status} />
                          </td>
                          <td style={{ padding: '7px 10px', color: 'var(--text-muted)', fontVariantNumeric: 'tabular-nums' }}>
                            {formatMs(app.duration_ms)}
                          </td>
                          <td style={{ padding: '7px 10px', color: 'var(--text-dim)' }}>
                            {app.submit_date ?? '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {(!spark?.active_apps?.length && !spark?.completed_apps?.length) && (
              <div
                style={{
                  padding: '24px 0',
                  textAlign: 'center',
                  color: 'var(--text-dim)',
                  fontSize: '0.82rem',
                }}
              >
                No Spark jobs yet — Spark ETL jobs will appear here automatically after repository ingestion.
              </div>
            )}
          </>
        )}
      </div>

      {/* ── Quick Links ── */}
      <div
        style={{
          display: 'flex',
          gap: 12,
          flexWrap: 'wrap',
        }}
      >
        {[
          { label: 'Spark Master UI',    url: 'http://localhost:8080', icon: <Zap size={13} /> },
          { label: 'HDFS NameNode UI',   url: 'http://localhost:9870', icon: <HardDrive size={13} /> },
          { label: 'Kafka UI',           url: 'http://localhost:8090', icon: <Server size={13} /> },
        ].map(({ label, url, icon }) => (
          <a
            key={label}
            href={url}
            target="_blank"
            rel="noreferrer"
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 6,
              padding: '7px 12px',
              borderRadius: 'var(--radius-xs)',
              border: '1px solid var(--border-subtle)',
              background: 'rgba(255,255,255,0.02)',
              color: 'var(--text-secondary)',
              fontSize: '0.78rem',
              textDecoration: 'none',
              transition: 'border-color 0.15s, color 0.15s',
            }}
            onMouseEnter={e => {
              const el = e.currentTarget as HTMLAnchorElement
              el.style.borderColor = 'var(--border-primary)'
              el.style.color = 'var(--text-primary)'
            }}
            onMouseLeave={e => {
              const el = e.currentTarget as HTMLAnchorElement
              el.style.borderColor = 'var(--border-subtle)'
              el.style.color = 'var(--text-secondary)'
            }}
          >
            {icon}
            <span>{label}</span>
          </a>
        ))}
      </div>
    </div>
  )
}
