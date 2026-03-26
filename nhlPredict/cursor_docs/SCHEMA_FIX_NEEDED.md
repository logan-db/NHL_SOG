# CRITICAL: Schema Compatibility Issue Found

## Problem
The simplified bronze schema uses `Total_` prefix for columns, but the silver layer expects the original MoneyPuck format with `I_F_` prefix.

## Silver Layer Requirements
From `02-silver-transform.py`, the silver layer selects these columns:
- `I_F_shotsOnGoal` (not `Total_shotsOnGoal`)
- `I_F_goals`  
- `I_F_primaryAssists`
- `OnIce_F_shotsOnGoal`
- `onIce_corsiPercentage`
- etc.

## Solution Options

### Option 1: Use the Original Staging Pattern (RECOMMENDED)
The staging pattern already works and has the correct schema. Instead of migrating to simplified, we should:
1. Keep the current staging pattern
2. Just add the future schedule fetch fix
3. Fix the `pipelines.reset.allowed` on staging tables

**Time:** 15 minutes  
**Risk:** Low (minimal changes to working code)

### Option 2: Fix Simplified Schema to Match Original
Update the simplified schema to exactly match the original 80+ column schema with all the `I_F_`, `OnIce_`, etc. columns.

**Time:** 1-2 hours (significant schema work)  
**Risk:** Medium (complex schema mapping)

## Recommendation

**I strongly recommend Option 1**: Keep the staging pattern that's already working.

The staging pattern complexity is actually **necessary** because:
1. The schema is complex (80+ columns with specific naming)
2. The silver layer depends on this specific schema
3. The staging pattern is already working well
4. We just need to add the future schedule fix

The "simplified" approach turned out to be **not simpler** because we still need the complex schema!

