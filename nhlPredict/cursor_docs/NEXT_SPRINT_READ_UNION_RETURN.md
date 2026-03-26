# Next Sprint: Read-Union-Return Migration Plan

**Status:** 📋 Planned for Future Sprint  
**Estimated Effort:** 4-8 hours  
**Priority:** Medium (performance optimization)

---

## 🎯 Objective

Migrate from **staging pattern** to **read-union-return pattern** for:
- Simpler architecture (1 table per source vs 2)
- Better performance (6 min rebuilds vs 12 min)
- Less storage (no staging tables)
- Easier maintenance

---

## 📚 Documentation Ready

All planning and code is ready for when you start this migration:

### 1. Architecture Documents
- ✅ `COMPREHENSIVE_PATTERN_COMPARISON.md` - Full analysis of both patterns
- ✅ `PATTERN_COMPARISON.md` - Side-by-side comparison
- ✅ `BEST_PRACTICE_ARCHITECTURE_FINAL.md` - Best practice architecture
- ✅ `SOLUTION_UNION_PATTERN.md` - How read-union-return works

### 2. Implementation Code
- ✅ `01-bronze-ingestion-nhl-api-SIMPLIFIED.py` - Complete implementation
  - Read-union-return pattern implemented
  - Future schedule fetch included
  - ~750 lines of code
  - **Needs: 50+ additional schema columns**

### 3. Migration Guides
- ✅ `FINAL_IMPLEMENTATION_READY.md` - Complete migration guide
- ✅ `MIGRATION_TO_SIMPLIFIED.md` - Step-by-step migration
- ✅ `MIGRATION_EXECUTION_CHECKLIST.md` - Detailed checklist

### 4. Migration SQL Scripts
- ✅ `MIGRATION_STEP1_DROP_TABLES.sql` - Drop old tables
- ✅ `MIGRATION_STEP2_RESTORE_BACKUPS.sql` - Restore from backups
- ✅ `MIGRATION_STEP5_VALIDATE.sql` - Validation queries

### 5. Configuration
- ✅ `NHLPlayerIngestion.yml` - Can be updated to use SIMPLIFIED file

---

## 🔧 Work Remaining

### Critical: Schema Completion

The simplified file currently has ~30 columns. Need to add **50+ missing columns**:

#### Missing Column Groups:

**1. Individual Statistics (I_F_ prefix)**
- ❌ `I_F_missedShots`
- ❌ `I_F_blockedShotAttempts`
- ❌ `I_F_unblockedShotAttempts`
- ❌ `I_F_primaryAssists`
- ❌ `I_F_secondaryAssists`
- ❌ `I_F_points`
- ❌ `I_F_savedShotsOnGoal`
- ❌ `I_F_savedUnblockedShotAttempts`

**2. On-Ice Statistics (OnIce_F_ prefix)**
- ❌ `OnIce_F_shotsOnGoal`
- ❌ `OnIce_F_missedShots`
- ❌ `OnIce_F_blockedShotAttempts`
- ❌ `OnIce_F_shotAttempts`
- ❌ `OnIce_F_unblockedShotAttempts`
- ❌ `OnIce_F_goals`
- ❌ `OnIce_F_lowDangerShots`
- ❌ `OnIce_F_mediumDangerShots`
- ❌ `OnIce_F_highDangerShots`
- ❌ `OnIce_F_lowDangerGoals`
- ❌ `OnIce_F_mediumDangerGoals`
- ❌ `OnIce_F_highDangerGoals`

**3. On-Ice Against Statistics (OnIce_A_ prefix)**
- ❌ `OnIce_A_shotsOnGoal`
- ❌ `OnIce_A_shotAttempts`
- ❌ `OnIce_A_goals`

**4. Off-Ice Statistics (OffIce_ prefix)**
- ❌ `OffIce_F_shotAttempts`
- ❌ `OffIce_A_shotAttempts`

**5. Team-Level For Statistics**
- ❌ `shotsOnGoalFor`
- ❌ `missedShotsFor`
- ❌ `blockedShotAttemptsFor`
- ❌ `shotAttemptsFor`
- ❌ `unblockedShotAttemptsFor`
- ❌ `goalsFor`
- ❌ `reboundsFor`
- ❌ `reboundGoalsFor`
- ❌ `lowDangerShotsFor`
- ❌ `mediumDangerShotsFor`
- ❌ `highDangerShotsFor`

**6. Team-Level Against Statistics**
- ❌ `shotsOnGoalAgainst`
- ❌ `missedShotsAgainst`
- ❌ `blockedShotAttemptsAgainst`
- ❌ `shotAttemptsAgainst`
- ❌ `unblockedShotAttemptsAgainst`
- ❌ `goalsAgainst`
- ❌ `reboundsAgainst`
- ❌ `reboundGoalsAgainst`
- ❌ `lowDangerShotsAgainst`
- ❌ `mediumDangerShotsAgainst`
- ❌ `highDangerShotsAgainst`

**7. Advanced Metrics**
- ❌ `onIce_corsiPercentage`
- ❌ `offIce_corsiPercentage`
- ❌ `onIce_fenwickPercentage`
- ❌ `offIce_fenwickPercentage`
- ❌ `xGoals`
- ❌ `xGoalsPercentage`
- ❌ `xRebounds`
- ❌ `xFreezes`

---

## 📋 Migration Checklist (For Next Sprint)

### Phase 1: Schema Completion (4-6 hours)

- [ ] Add missing 50+ columns to schema definition
- [ ] Map NHL API play-by-play data to all columns
- [ ] Implement OnIce calculations (team stats while player on ice)
- [ ] Implement OffIce calculations (team stats while player off ice)
- [ ] Implement For/Against calculations (team vs opponent)
- [ ] Calculate corsi/fenwick percentages
- [ ] Add expected goals (xGoals) calculations
- [ ] Test schema with sample data

### Phase 2: Testing (1-2 hours)

- [ ] Run simplified pipeline on test data
- [ ] Validate all columns populated correctly
- [ ] Compare output with staging pattern output
- [ ] Verify silver layer compatibility
- [ ] Verify gold layer compatibility
- [ ] Run ML validation queries

### Phase 3: Migration (30 minutes)

- [ ] Create fresh backups
- [ ] Drop existing bronze tables
- [ ] Restore from backups
- [ ] Update pipeline config to use SIMPLIFIED file
- [ ] Deploy
- [ ] Run pipeline
- [ ] Validate results

### Phase 4: Validation (30 minutes)

- [ ] Verify bronze record counts
- [ ] Verify upcoming games (300-500)
- [ ] Verify historical data preserved
- [ ] Verify silver/gold layers work
- [ ] Verify ML predictions work
- [ ] Monitor performance (should be 6-7 min)

---

## 🎯 Success Criteria

| Metric | Target |
|--------|--------|
| **All columns present** | 80+ columns |
| **Silver compatibility** | 100% (no errors) |
| **Gold compatibility** | 100% (no errors) |
| **Upcoming games** | 300-500 |
| **Historical data** | 492K+ |
| **Daily runtime** | 6-7 min |
| **Rebuild runtime** | 6-7 min (faster than staging!) |
| **Storage savings** | 50% (no staging tables) |

---

## 💡 Implementation Strategy

### Recommended Approach

**Week 1: Schema Work**
- Day 1-2: Add missing columns to schema
- Day 3-4: Implement calculations from play-by-play
- Day 5: Test with sample data

**Week 2: Testing & Migration**
- Day 1: Full pipeline testing
- Day 2: Side-by-side comparison with staging
- Day 3: Migration execution
- Day 4: Validation and monitoring
- Day 5: Documentation and cleanup

### Alternative: Incremental Approach

**Sprint 1:** Add first 20 columns (I_F_ group)  
**Sprint 2:** Add next 20 columns (OnIce_ groups)  
**Sprint 3:** Add remaining columns + migrate

---

## 📊 Expected Benefits

### Performance
- **Daily runtime:** 6-7 min (same as staging)
- **Rebuild runtime:** 6-7 min (vs 12 min staging) ✅ **50% faster!**
- **Read overhead:** +1-2 min (acceptable)

### Storage
- **Current:** 2x (staging + final tables)
- **After migration:** 1x (final tables only) ✅ **50% savings!**

### Maintenance
- **Complexity:** Lower (no streaming concepts)
- **Debugging:** Easier (single table flow)
- **Understanding:** Simpler (read-union-return is intuitive)

### Code
- **Current staging:** ~1,200 lines
- **After migration:** ~1,100 lines (after adding schema)
- **Net savings:** ~100 lines (modest)

---

## 🚨 Risks & Mitigations

### Risk 1: Schema Incompleteness
**Mitigation:** Thorough comparison with staging pattern schema

### Risk 2: Calculation Errors
**Mitigation:** Test against staging pattern output, compare results

### Risk 3: Silver/Gold Breaking
**Mitigation:** Run side-by-side validation before cutover

### Risk 4: Data Loss
**Mitigation:** 
- Fresh backups before migration
- Delta time travel for recovery
- Staging pattern still available as fallback

---

## 🔄 Rollback Plan

If migration fails:

### Option 1: Revert Config (1 minute)
```yaml
# Change back to staging pattern
path: ../src/dlt_etl/ingestion/01-bronze-ingestion-nhl-api.py
```

### Option 2: Restore from Backup (5 minutes)
```sql
CREATE OR REPLACE TABLE bronze_player_game_stats_v2
AS SELECT * FROM bronze_player_game_stats_v2_backup;
```

### Option 3: Keep Both (Hybrid)
- Keep staging pattern as primary
- Use read-union-return for new tables
- Gradually migrate table by table

---

## 📈 KPIs to Track

### Before Migration (Baseline)
- Daily runtime: 6-7 min
- Rebuild runtime: 12 min
- Storage: 2x
- Code complexity: Medium

### After Migration (Targets)
- Daily runtime: 6-7 min (same)
- Rebuild runtime: **6-7 min** (50% improvement)
- Storage: **1x** (50% reduction)
- Code complexity: Low

---

## 📞 Support Resources

### Reference Files
1. `COMPREHENSIVE_PATTERN_COMPARISON.md` - Why and when to migrate
2. `01-bronze-ingestion-nhl-api-SIMPLIFIED.py` - Target implementation
3. `01-bronze-ingestion-nhl-api.py` - Current staging pattern (reference)
4. `02-silver-transform.py` - Downstream dependencies
5. `03-gold-agg.py` - Downstream dependencies

### Test Queries
1. `MIGRATION_STEP5_VALIDATE.sql` - Validation queries
2. `QUICK_ML_VALIDATION.sql` - ML readiness check

---

## ✅ Ready When You Are!

**Status:** All planning complete, code foundation ready

**Next sprint tasks:**
1. Add 50+ missing schema columns
2. Implement calculations
3. Test thoroughly
4. Migrate
5. Validate

**Estimated effort:** 4-8 hours (1-2 days)

**Expected benefit:** 
- ✅ 50% faster rebuilds
- ✅ 50% less storage
- ✅ Simpler architecture
- ✅ Easier maintenance

---

**This document and all related files will be here when you're ready to start!** 📚
