import React, { useCallback, useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import {
  getHyperliquidAvailableSymbols,
  getHyperliquidWatchlist,
  updateHyperliquidWatchlist,
} from '@/lib/api'
import type { HyperliquidSymbolMeta } from '@/lib/api'
import DataCoverageHeatmap from './DataCoverageHeatmap'

interface StorageStats {
  total_size_mb: number
  tables: Record<string, number>
  retention_days: number
  symbol_count: number
  estimated_per_symbol_per_day_mb: number
}

export default function SettingsPage() {
  const { t, i18n } = useTranslation()
  const [activeTab, setActiveTab] = useState('watchlist')

  // Language state
  const currentLang = 'en'

  // Watchlist state
  const [availableSymbols, setAvailableSymbols] = useState<HyperliquidSymbolMeta[]>([])
  const [watchlistSymbols, setWatchlistSymbols] = useState<string[]>([])
  const [maxWatchlistSymbols, setMaxWatchlistSymbols] = useState(10)
  const [watchlistLoading, setWatchlistLoading] = useState(true)
  const [watchlistSaving, setWatchlistSaving] = useState(false)
  const [watchlistError, setWatchlistError] = useState<string | null>(null)
  const [watchlistSuccess, setWatchlistSuccess] = useState<string | null>(null)

  // Storage stats state
  const [storageStats, setStorageStats] = useState<StorageStats | null>(null)
  const [storageLoading, setStorageLoading] = useState(false)
  const [retentionDays, setRetentionDays] = useState<string>('365')
  const [retentionSaving, setRetentionSaving] = useState(false)
  const [retentionError, setRetentionError] = useState<string | null>(null)
  const [retentionSuccess, setRetentionSuccess] = useState<string | null>(null)

  const toggleLanguage = (lang: 'en') => {
    i18n.changeLanguage(lang)
  }

  const fetchWatchlist = useCallback(async () => {
    setWatchlistLoading(true)
    setWatchlistError(null)
    try {
      const [available, watchlist] = await Promise.all([
        getHyperliquidAvailableSymbols(),
        getHyperliquidWatchlist(),
      ])
      setAvailableSymbols(available.symbols || [])
      setMaxWatchlistSymbols(watchlist.max_symbols ?? 10)
      setWatchlistSymbols(watchlist.symbols || [])
    } catch (err) {
      setWatchlistError(err instanceof Error ? err.message : 'Failed to load watchlist')
    } finally {
      setWatchlistLoading(false)
    }
  }, [])

  const fetchStorageStats = useCallback(async () => {
    setStorageLoading(true)
    try {
      const res = await fetch('/api/system/storage-stats')
      if (res.ok) {
        const data: StorageStats = await res.json()
        setStorageStats(data)
        setRetentionDays(data.retention_days.toString())
      }
    } catch (err) {
      console.error('Failed to fetch storage stats:', err)
    } finally {
      setStorageLoading(false)
    }
  }, [])

  // Load watchlist on mount
  useEffect(() => {
    fetchWatchlist()
  }, [fetchWatchlist])

  // Load storage stats only when Exchange Data tab is active
  useEffect(() => {
    if (activeTab === 'exchange-data' && !storageStats) {
      fetchStorageStats()
    }
  }, [activeTab, storageStats, fetchStorageStats])

  const toggleWatchlistSymbol = (symbol: string) => {
    const symbolUpper = symbol.toUpperCase()
    setWatchlistError(null)
    setWatchlistSuccess(null)
    setWatchlistSymbols((prev) => {
      if (prev.includes(symbolUpper)) {
        return prev.filter((s) => s !== symbolUpper)
      }
      if (prev.length >= maxWatchlistSymbols) {
        setWatchlistError(t('settings.maxSymbolsReached', `Maximum ${maxWatchlistSymbols} symbols`))
        return prev
      }
      return [...prev, symbolUpper]
    })
  }

  const handleSaveWatchlist = async () => {
    setWatchlistSaving(true)
    setWatchlistError(null)
    setWatchlistSuccess(null)
    try {
      await updateHyperliquidWatchlist(watchlistSymbols)
      setWatchlistSuccess(t('settings.watchlistSaved', 'Watchlist saved'))
    } catch (err) {
      setWatchlistError(err instanceof Error ? err.message : 'Failed to save')
    } finally {
      setWatchlistSaving(false)
    }
  }

  const handleSaveRetention = async () => {
    const days = parseInt(retentionDays, 10)
    if (isNaN(days) || days < 7 || days > 730) {
      setRetentionError(t('settings.retentionRange', 'Must be between 7 and 730 days'))
      return
    }
    setRetentionSaving(true)
    setRetentionError(null)
    setRetentionSuccess(null)
    try {
      const res = await fetch('/api/system/retention-days', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ days }),
      })
      if (!res.ok) throw new Error('Failed to update')
      setRetentionSuccess(t('settings.retentionSaved', 'Retention updated'))
      if (storageStats) {
        setStorageStats({ ...storageStats, retention_days: days })
      }
    } catch (err) {
      setRetentionError(err instanceof Error ? err.message : 'Failed to save')
    } finally {
      setRetentionSaving(false)
    }
  }

  // Calculate max storage estimate using watchlist symbol count
  const maxStorageEstimate = storageStats
    ? (watchlistSymbols.length * parseInt(retentionDays, 10) * storageStats.estimated_per_symbol_per_day_mb).toFixed(1)
    : '—'

  return (
    <div className="p-6 h-[calc(100vh-64px)] flex flex-col overflow-hidden">
      {/* Language Settings - Compact row with border */}
      <div className="flex items-center gap-3 mb-6 shrink-0 p-4 border rounded-lg bg-card">
        <span className="text-sm font-medium">{t('settings.language', 'Language')}</span>
        <select
          value={currentLang}
          onChange={(e) => toggleLanguage(e.target.value as 'en')}
          className="border rounded px-2 py-1 text-sm bg-background"
        >
          <option value="en">English</option>
        </select>
      </div>

      {/* Tabs: Watchlist | Exchange Data */}
      <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col min-h-0">
        <TabsList className="grid w-full grid-cols-2 max-w-md shrink-0">
          <TabsTrigger value="watchlist">{t('settings.watchlist', 'Watchlist')}</TabsTrigger>
          <TabsTrigger value="exchange-data">{t('settings.exchangeData', 'Exchange Data')}</TabsTrigger>
        </TabsList>

        {/* Watchlist Tab */}
        <TabsContent value="watchlist" className="mt-4 flex-1 min-h-0 flex flex-col">
          <Card className="flex flex-col flex-1 min-h-0">
            <CardHeader className="shrink-0">
              <CardTitle>{t('settings.watchlist', 'Watchlist')}</CardTitle>
              <CardDescription>
                {t('settings.watchlistDesc', 'Select symbols for market data collection and AI analysis')}
              </CardDescription>
            </CardHeader>
            <CardContent className="flex-1 overflow-y-auto min-h-0">
              {watchlistLoading ? (
                <div className="text-muted-foreground">{t('common.loading', 'Loading...')}</div>
              ) : (
                <>
                  <div className="text-sm text-muted-foreground mb-3">
                    {t('settings.selectedCount', 'Selected')}: {watchlistSymbols.length} / {maxWatchlistSymbols}
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {availableSymbols.map((sym) => {
                      const isSelected = watchlistSymbols.includes(sym.name.toUpperCase())
                      return (
                        <Button
                          key={sym.name}
                          variant={isSelected ? 'default' : 'outline'}
                          size="sm"
                          onClick={() => toggleWatchlistSymbol(sym.name)}
                        >
                          {sym.name}
                        </Button>
                      )
                    })}
                  </div>
                </>
              )}
            </CardContent>
            <CardFooter className="shrink-0 border-t pt-4 flex items-center gap-3">
              <Button
                onClick={handleSaveWatchlist}
                disabled={watchlistSaving || watchlistLoading}
              >
                {watchlistSaving ? t('common.saving', 'Saving...') : t('common.save', 'Save')}
              </Button>
              {watchlistError && (
                <span className="text-red-500 text-sm">{watchlistError}</span>
              )}
              {watchlistSuccess && (
                <span className="text-green-500 text-sm">{watchlistSuccess}</span>
              )}
            </CardFooter>
          </Card>
        </TabsContent>

        {/* Exchange Data Tab */}
        <TabsContent value="exchange-data" className="mt-4 flex-1 min-h-0 flex flex-col">
          <Card className="flex flex-col flex-1 min-h-0">
            <CardHeader className="shrink-0">
              <CardTitle>{t('settings.dataCollection', 'Data Collection')}</CardTitle>
              <CardDescription>
                {t('settings.dataCollectionDesc', 'Market flow data storage statistics')}
              </CardDescription>
            </CardHeader>
            <CardContent className="flex-1 overflow-y-auto min-h-0 space-y-6">
              {storageLoading ? (
                <div className="text-muted-foreground">{t('common.loading', 'Loading...')}</div>
              ) : storageStats ? (
                <div className="space-y-4">
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div>
                      <div className="text-sm text-muted-foreground">
                        {t('settings.currentStorage', 'Current Storage')}
                      </div>
                      <div className="text-xl font-semibold">{storageStats.total_size_mb} MB</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground">
                        {t('settings.collectedSymbols', 'Collected Symbols')}
                      </div>
                      <div className="text-xl font-semibold">{storageStats.symbol_count}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground">
                        {t('settings.retentionDays', 'Retention Days')}
                      </div>
                      <div className="text-xl font-semibold">{storageStats.retention_days}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground">
                        {t('settings.maxStorageEstimate', 'Max Storage Estimate')}
                      </div>
                      <div className="text-xl font-semibold">{maxStorageEstimate} MB</div>
                    </div>
                  </div>

                  {/* Retention Days Setting */}
                  <div className="pt-4 border-t">
                    <div className="text-sm font-medium mb-2">
                      {t('settings.setRetention', 'Set Retention Period')}
                    </div>
                    <div className="flex items-center gap-2">
                      <Input
                        type="number"
                        value={retentionDays}
                        onChange={(e) => setRetentionDays(e.target.value)}
                        className="w-24"
                        min={7}
                        max={730}
                      />
                      <span className="text-sm text-muted-foreground">{t('settings.days', 'days')}</span>
                      <Button onClick={handleSaveRetention} disabled={retentionSaving} size="sm">
                        {retentionSaving ? t('common.saving', 'Saving...') : t('common.save', 'Save')}
                      </Button>
                    </div>
                    {retentionError && <div className="text-red-500 text-sm mt-2">{retentionError}</div>}
                    {retentionSuccess && <div className="text-green-500 text-sm mt-2">{retentionSuccess}</div>}
                    <div className="text-xs text-muted-foreground mt-1">
                      {t('settings.retentionHint', 'Data older than this will be automatically cleaned up (7-730 days)')}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="text-muted-foreground">{t('settings.noData', 'No data available')}</div>
              )}

              {/* Data Coverage Heatmap - inside same Card */}
              <div className="pt-4 border-t">
                <div className="text-sm font-medium mb-3">{t('settings.dataCoverage', 'Data Coverage')}</div>
                <DataCoverageHeatmap />
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  )
}
