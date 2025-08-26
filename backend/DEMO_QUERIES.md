# Agricultural Data Platform - Demo Queries (v1.1.4)

## Showcase Queries for Client Demo

### 1. Temporal Analysis & Growth Trends
**Query:** "Show me agricultural performance trends in the Midwest over the past 5 years"
- **Highlights:** Multi-year analysis, growth rate calculations, trend identification
- **Graph DB Advantage:** Shows GROWING/DECLINING/STABLE trends with year-over-year percentages

### 2. Geographic Relationships & Network Effects
**Query:** "Which states border Iowa and how do their agricultural economies compare"
- **Highlights:** BORDERS relationships from database, economic comparisons
- **Graph DB Advantage:** Shows HIGHER_INCOME_THAN, MORE_EFFICIENT_THAN, SIMILAR_ECONOMY_TO relationships

### 3. Agricultural Belt Analysis
**Query:** "Compare corn belt states agricultural performance"
- **Highlights:** IN_CORN_BELT relationships, regional patterns
- **Graph DB Advantage:** Identifies states in same agricultural belt with shared characteristics

### 4. Economic Performance Clustering
**Query:** "Find states with similar agricultural economies to California"
- **Highlights:** SIMILAR_ECONOMY_TO relationships with income/expense ratios
- **Graph DB Advantage:** Discovers economic patterns through relationship analysis

### 5. Supply Chain Impact Analysis
**Query:** "Show agricultural relationships and dependencies for Texas"
- **Highlights:** Multiple relationship types, comprehensive network view
- **Graph DB Advantage:** Reveals complex interconnections not visible in tabular data

## Key Features Demonstrated

### v1.1.4 Enhancements:
- ✅ Temporal queries mapped to available years (1997, 2002, 2007, 2012, 2017, 2022)
- ✅ Year-over-year growth calculations with percentages
- ✅ Performance trends (GROWING/DECLINING/STABLE)
- ✅ Actual BORDERS relationships from Neo4j database
- ✅ Economic comparisons (HIGHER_INCOME_THAN, MORE_EFFICIENT_THAN)
- ✅ Agricultural belt relationships (IN_CORN_BELT, IN_WHEAT_BELT)
- ✅ Regional groupings (IN_REGION, SHARES_REGION_WITH)

### Visual Differentiation:
- SQL: Traditional ASCII table with rows and columns
- Graph: Neo4j-style table with Labels, Names, Properties, and Relationships columns
- Graph shows directional relationship indicators (→, ←, ↔)

### AI Interpretation:
- Focus on agricultural insights, not database technology
- Quantitative analysis with specific numbers and percentages
- Clear business value differentiation between datasets

## Quick Test Commands

```bash
# Test backend health
curl http://localhost:8000/health

# Test a complex query
curl -X POST http://localhost:8000/api/v1/search \
  -H "Content-Type: application/json" \
  -d '{"query": "Show me agricultural performance trends in the Midwest over the past 5 years"}'
```

## Demo Tips
1. Start with temporal query to show multi-year analysis
2. Follow with geographic relationships to show network effects
3. Highlight the relationships column in graph output
4. Point out growth percentages and trend indicators
5. Emphasize how graph reveals hidden connections SQL misses