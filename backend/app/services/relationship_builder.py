"""
Relationship builder for creating meaningful graph relationships.
Analyzes data to create dynamic relationships beyond basic HAS_MEASUREMENT.
"""

from typing import List, Dict, Any, Tuple
from app.core.logging import app_logger


class RelationshipBuilder:
    """Builds meaningful relationships between nodes based on data analysis."""
    
    def __init__(self):
        """Initialize the relationship builder."""
        # Geographic relationships - which states border each other (expanded)
        self.state_borders = {
            'Iowa': ['Minnesota', 'Wisconsin', 'Illinois', 'Missouri', 'Nebraska', 'South Dakota'],
            'California': ['Oregon', 'Nevada', 'Arizona'],
            'Texas': ['New Mexico', 'Oklahoma', 'Arkansas', 'Louisiana'],
            'Illinois': ['Wisconsin', 'Indiana', 'Kentucky', 'Missouri', 'Iowa'],
            'Nebraska': ['South Dakota', 'Iowa', 'Missouri', 'Kansas', 'Colorado', 'Wyoming'],
            'Minnesota': ['Wisconsin', 'Iowa', 'South Dakota', 'North Dakota'],
            'Wisconsin': ['Michigan', 'Minnesota', 'Iowa', 'Illinois'],
            'Michigan': ['Ohio', 'Indiana', 'Wisconsin'],
            'Ohio': ['Michigan', 'Indiana', 'Kentucky', 'West Virginia', 'Pennsylvania'],
            'Indiana': ['Michigan', 'Ohio', 'Kentucky', 'Illinois'],
            'Missouri': ['Iowa', 'Illinois', 'Kentucky', 'Tennessee', 'Arkansas', 'Oklahoma', 'Kansas', 'Nebraska'],
            'Kansas': ['Nebraska', 'Missouri', 'Oklahoma', 'Colorado'],
            'North Dakota': ['Minnesota', 'South Dakota', 'Montana'],
            'South Dakota': ['North Dakota', 'Minnesota', 'Iowa', 'Nebraska', 'Wyoming', 'Montana'],
            'Florida': ['Georgia', 'Alabama'],
            'Georgia': ['Florida', 'Alabama', 'Tennessee', 'North Carolina', 'South Carolina'],
            'New York': ['Vermont', 'Massachusetts', 'Connecticut', 'New Jersey', 'Pennsylvania'],
            'Pennsylvania': ['New York', 'New Jersey', 'Delaware', 'Maryland', 'West Virginia', 'Ohio'],
            'Colorado': ['Wyoming', 'Nebraska', 'Kansas', 'Oklahoma', 'New Mexico', 'Arizona', 'Utah'],
            'Arizona': ['California', 'Nevada', 'Utah', 'Colorado', 'New Mexico'],
            'Washington': ['Idaho', 'Oregon'],
            'Oregon': ['Washington', 'Idaho', 'Nevada', 'California'],
            # States without land borders
            'Alaska': [],  # No US state borders
            'Hawaii': [],  # Island state
        }
        
        # Agricultural belts
        self.corn_belt = ['Iowa', 'Illinois', 'Indiana', 'Ohio', 'Nebraska', 'Minnesota', 'Wisconsin']
        self.wheat_belt = ['Kansas', 'Oklahoma', 'Texas', 'Nebraska', 'Colorado']
        self.cotton_belt = ['Texas', 'Georgia', 'Mississippi', 'Arkansas', 'Louisiana', 'Alabama']
        
        # Regions
        self.regions = {
            'Midwest': ['Iowa', 'Illinois', 'Indiana', 'Michigan', 'Minnesota', 'Missouri', 'Ohio', 'Wisconsin'],
            'South': ['Texas', 'Florida', 'Georgia', 'Virginia', 'North Carolina', 'South Carolina', 'Alabama', 'Mississippi', 'Louisiana', 'Arkansas', 'Tennessee', 'Kentucky'],
            'West': ['California', 'Oregon', 'Washington', 'Nevada', 'Arizona', 'Utah', 'Colorado', 'New Mexico'],
            'Northeast': ['New York', 'Pennsylvania', 'New Jersey', 'Massachusetts', 'Connecticut', 'Rhode Island', 'Vermont', 'New Hampshire', 'Maine']
        }
    
    def build_geographic_relationships(self, state_name: str) -> List[Dict[str, Any]]:
        """
        Build geographic relationships for a state.
        
        Args:
            state_name: Name of the state
            
        Returns:
            List of relationship dictionaries
        """
        relationships = []
        
        # BORDERS relationships
        if state_name in self.state_borders:
            for neighbor in self.state_borders[state_name]:
                relationships.append({
                    'type': 'BORDERS',
                    'target': neighbor,
                    'properties': {'shared_border': True}
                })
        
        # IN_REGION relationships
        for region, states in self.regions.items():
            if state_name in states:
                relationships.append({
                    'type': 'IN_REGION',
                    'target': region,
                    'properties': {'region_name': region}
                })
                
                # SHARES_REGION_WITH relationships
                for other_state in states:
                    if other_state != state_name:
                        relationships.append({
                            'type': 'SHARES_REGION_WITH',
                            'target': other_state,
                            'properties': {'region': region}
                        })
        
        # Agricultural belt relationships
        if state_name in self.corn_belt:
            relationships.append({
                'type': 'IN_CORN_BELT',
                'target': 'Corn Belt',
                'properties': {'primary_crop': 'corn'}
            })
        
        if state_name in self.wheat_belt:
            relationships.append({
                'type': 'IN_WHEAT_BELT',
                'target': 'Wheat Belt',
                'properties': {'primary_crop': 'wheat'}
            })
        
        if state_name in self.cotton_belt:
            relationships.append({
                'type': 'IN_COTTON_BELT',
                'target': 'Cotton Belt',
                'properties': {'primary_crop': 'cotton'}
            })
        
        return relationships
    
    def build_economic_relationships(self, state_data: Dict[str, Any], other_states: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Build economic relationships by comparing metrics.
        
        Args:
            state_data: Data for the primary state
            other_states: Data for other states to compare
            
        Returns:
            List of relationship dictionaries
        """
        relationships = []
        
        state_name = state_data.get('name')
        state_income = state_data.get('income_value', 0)
        state_expenses = state_data.get('expense_value', 0)
        
        for other in other_states:
            other_name = other.get('name')
            other_income = other.get('income_value', 0)
            other_expenses = other.get('expense_value', 0)
            
            if other_name == state_name:
                continue
            
            # Income comparisons
            if state_income > other_income * 1.2:  # 20% higher
                relationships.append({
                    'type': 'HIGHER_INCOME_THAN',
                    'target': other_name,
                    'properties': {
                        'difference': state_income - other_income,
                        'percentage': round((state_income - other_income) / other_income * 100, 2) if other_income > 0 else 0
                    }
                })
            elif other_income > state_income * 1.2:  # 20% lower
                relationships.append({
                    'type': 'LOWER_INCOME_THAN',
                    'target': other_name,
                    'properties': {
                        'difference': other_income - state_income,
                        'percentage': round((other_income - state_income) / state_income * 100, 2) if state_income > 0 else 0
                    }
                })
            
            # Expense comparisons
            if state_expenses < other_expenses * 0.8:  # 20% lower expenses
                relationships.append({
                    'type': 'MORE_EFFICIENT_THAN',
                    'target': other_name,
                    'properties': {
                        'expense_difference': other_expenses - state_expenses,
                        'efficiency_ratio': round(state_expenses / other_expenses, 2) if other_expenses > 0 else 0
                    }
                })
            
            # Similar economy detection (within 10% for both metrics)
            income_ratio = state_income / other_income if other_income > 0 else 0
            expense_ratio = state_expenses / other_expenses if other_expenses > 0 else 0
            
            if 0.9 <= income_ratio <= 1.1 and 0.9 <= expense_ratio <= 1.1:
                relationships.append({
                    'type': 'SIMILAR_ECONOMY_TO',
                    'target': other_name,
                    'properties': {
                        'income_similarity': round(income_ratio, 2),
                        'expense_similarity': round(expense_ratio, 2)
                    }
                })
        
        return relationships
    
    def build_performance_relationships(self, state_metrics: Dict[str, Any], year: int) -> List[Dict[str, Any]]:
        """
        Build performance-based relationships.
        
        Args:
            state_metrics: Metrics for the state
            year: Year of the metrics
            
        Returns:
            List of relationship dictionaries
        """
        relationships = []
        
        # Calculate profit margin
        income = state_metrics.get('income', 0)
        expenses = state_metrics.get('expenses', 0)
        profit_margin = (income - expenses) / income if income > 0 else 0
        
        # Performance categories
        if profit_margin > 0.3:  # 30% profit margin
            relationships.append({
                'type': 'HIGH_PERFORMER',
                'target': f'Year_{year}',
                'properties': {
                    'profit_margin': round(profit_margin * 100, 2),
                    'category': 'excellent'
                }
            })
        elif profit_margin > 0.15:  # 15% profit margin
            relationships.append({
                'type': 'GOOD_PERFORMER',
                'target': f'Year_{year}',
                'properties': {
                    'profit_margin': round(profit_margin * 100, 2),
                    'category': 'good'
                }
            })
        elif profit_margin < 0:  # Negative margin
            relationships.append({
                'type': 'UNDERPERFORMER',
                'target': f'Year_{year}',
                'properties': {
                    'profit_margin': round(profit_margin * 100, 2),
                    'category': 'struggling'
                }
            })
        
        return relationships
    
    def build_temporal_relationships(self, current_year_data: Dict[str, Any], previous_year_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Build year-over-year temporal relationships.
        
        Args:
            current_year_data: Current year metrics
            previous_year_data: Previous year metrics
            
        Returns:
            List of relationship dictionaries
        """
        relationships = []
        
        current_income = current_year_data.get('income', 0)
        previous_income = previous_year_data.get('income', 0)
        current_expenses = current_year_data.get('expenses', 0)
        previous_expenses = previous_year_data.get('expenses', 0)
        
        # Income trends
        if current_income > previous_income * 1.1:  # 10% increase
            relationships.append({
                'type': 'INCOME_INCREASED_FROM',
                'target': str(previous_year_data.get('year')),
                'properties': {
                    'increase': current_income - previous_income,
                    'percentage': round((current_income - previous_income) / previous_income * 100, 2) if previous_income > 0 else 0
                }
            })
        elif current_income < previous_income * 0.9:  # 10% decrease
            relationships.append({
                'type': 'INCOME_DECREASED_FROM',
                'target': str(previous_year_data.get('year')),
                'properties': {
                    'decrease': previous_income - current_income,
                    'percentage': round((previous_income - current_income) / previous_income * 100, 2) if previous_income > 0 else 0
                }
            })
        
        # Expense trends
        if current_expenses > previous_expenses * 1.1:  # 10% increase
            relationships.append({
                'type': 'EXPENSES_INCREASED_FROM',
                'target': str(previous_year_data.get('year')),
                'properties': {
                    'increase': current_expenses - previous_expenses,
                    'percentage': round((current_expenses - previous_expenses) / previous_expenses * 100, 2) if previous_expenses > 0 else 0
                }
            })
        
        return relationships
    
    def format_relationships_for_display(self, relationships: List[Dict[str, Any]]) -> List[str]:
        """
        Format relationships for display in the graph table.
        
        Args:
            relationships: List of relationship dictionaries
            
        Returns:
            List of formatted relationship strings
        """
        formatted = []
        
        # Group by relationship type
        rel_counts = {}
        for rel in relationships:
            rel_type = rel['type']
            if rel_type not in rel_counts:
                rel_counts[rel_type] = 0
            rel_counts[rel_type] += 1
        
        # Format with directional arrows and counts
        for rel_type, count in rel_counts.items():
            if rel_type in ['BORDERS', 'IN_REGION', 'IN_CORN_BELT', 'IN_WHEAT_BELT', 'IN_COTTON_BELT', 
                           'HIGHER_INCOME_THAN', 'MORE_EFFICIENT_THAN', 'HIGH_PERFORMER']:
                formatted.append(f"→{rel_type}({count})")
            elif rel_type in ['LOWER_INCOME_THAN', 'UNDERPERFORMER']:
                formatted.append(f"←{rel_type}({count})")
            else:
                formatted.append(f"↔{rel_type}({count})")
        
        return formatted