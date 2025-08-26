"""
Graph result formatter for Neo4j query results.
Formats graph data into structured format for display.
"""

from typing import Dict, List, Any, Optional
from collections import defaultdict
from app.core.logging import app_logger
from app.services.relationship_builder import RelationshipBuilder


class GraphFormatter:
    """Formats Neo4j graph results for display with nodes, labels, and relationships."""
    
    def __init__(self):
        """Initialize the graph formatter."""
        self.relationship_builder = RelationshipBuilder()
    
    def format_for_display(self, graph_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Format graph results into a structured display format.
        
        Args:
            graph_results: Raw graph results from Neo4j
            
        Returns:
            List of formatted records for display
        """
        # If no graph structure, check if we can create pseudo-graph from node fields
        if "graph_structure" not in graph_results or not graph_results["graph_structure"].get("nodes"):
            # Check if the data has node fields (state_node, measurement_node, etc.)
            if graph_results.get("data") and len(graph_results["data"]) > 0:
                return self._format_from_node_fields(graph_results["data"])
            return graph_results.get("data", [])
        
        graph_structure = graph_results["graph_structure"]
        formatted_records = []
        
        # Process nodes
        nodes = graph_structure.get("nodes", {})
        relationships = graph_structure.get("relationships", [])
        
        # Build relationship index for quick lookup
        relationship_index = self._build_relationship_index(relationships)
        
        # Format each node with its relationships
        for node_id, node_data in nodes.items():
            # Calculate relationship counts by type
            outgoing_rels = defaultdict(int)
            incoming_rels = defaultdict(int)
            
            # Count outgoing relationships
            for rel in relationship_index.get(node_id, {}).get("outgoing", []):
                outgoing_rels[rel["type"]] += 1
            
            # Count incoming relationships
            for rel in relationship_index.get(node_id, {}).get("incoming", []):
                incoming_rels[rel["type"]] += 1
            
            # Extract primary label and name FIRST
            labels = node_data.get("labels", [])
            label_str = f"[:{':'.join(labels)}]" if labels else "[:Node]"
            
            # Get name from properties (try common name fields)
            props = node_data.get("properties", {})
            name = (props.get("name") or 
                   props.get("title") or 
                   props.get("label") or 
                   props.get("id") or 
                   node_id)
            
            # Format relationships for display - use meaningful relationships
            relationship_strs = []
            
            # Get state name for geographic relationships
            state_name = name
            
            # Add geographic relationships if this is a State node
            if "State" in labels:
                geo_rels = self.relationship_builder.build_geographic_relationships(state_name)
                
                # Group relationships by type to avoid duplicates
                rel_groups = {}
                for geo_rel in geo_rels:
                    rel_type = geo_rel['type']
                    if rel_type not in rel_groups:
                        rel_groups[rel_type] = []
                    rel_groups[rel_type].append(geo_rel.get('target', ''))
                
                # Format unique relationship types - prioritize agricultural belts
                priority_rels = []
                other_rels = []
                
                for rel_type, targets in rel_groups.items():
                    if rel_type in ['IN_CORN_BELT', 'IN_WHEAT_BELT', 'IN_COTTON_BELT']:
                        priority_rels.append(f"→{rel_type}(1)")
                    elif rel_type == 'BORDERS':
                        # Count unique border states
                        unique_borders = len(set(targets))
                        if unique_borders > 0:
                            other_rels.append(f"→BORDERS({unique_borders})")
                    elif rel_type == 'IN_REGION':
                        # Get region name from first target
                        if targets:
                            other_rels.append(f"→IN_{targets[0].upper()}(1)")
                    elif rel_type == 'SHARES_REGION_WITH':
                        # Count states in same region
                        other_rels.append(f"↔SHARES_REGION({len(set(targets))})")
                
                # Combine with priority relationships first
                relationship_strs = priority_rels + other_rels
                # Limit to most important relationships
                relationship_strs = relationship_strs[:3]
            
            # Add measurement relationships if this is a Measurement node
            elif "Measurement" in labels:
                metric_type = node_data.get("properties", {}).get("metric_type", "")
                if "Income" in metric_type:
                    relationship_strs.append("←MEASURES_INCOME(1)")
                elif "Expense" in metric_type:
                    relationship_strs.append("←MEASURES_EXPENSES(1)")
                else:
                    relationship_strs.append("←MEASURES(1)")
            
            # If no meaningful relationships found, use the basic ones
            if not relationship_strs:
                for rel_type, count in outgoing_rels.items():
                    if rel_type != 'HAS_MEASUREMENT':  # Skip generic relationship
                        relationship_strs.append(f"→{rel_type}({count})")
                for rel_type, count in incoming_rels.items():
                    if rel_type != 'HAS_MEASUREMENT':  # Skip generic relationship
                        relationship_strs.append(f"←{rel_type}({count})")
                
                # Only show HAS_MEASUREMENT if no other relationships exist
                if not relationship_strs:
                    if outgoing_rels.get('HAS_MEASUREMENT'):
                        relationship_strs.append(f"→HAS_MEASUREMENT({outgoing_rels['HAS_MEASUREMENT']})")
                    if incoming_rels.get('HAS_MEASUREMENT'):
                        relationship_strs.append(f"←HAS_MEASUREMENT({incoming_rels['HAS_MEASUREMENT']})")
            
            # Format properties for display (exclude name as it's shown separately)
            display_props = []
            for key, value in props.items():
                if key not in ["name", "title", "label", "id"]:
                    # Format numbers with commas if applicable
                    if isinstance(value, (int, float)):
                        if value >= 1000:
                            formatted_value = f"{value:,.0f}" if isinstance(value, int) else f"{value:,.2f}"
                        else:
                            formatted_value = str(value)
                    else:
                        formatted_value = str(value)
                    display_props.append(f"{key}: {formatted_value}")
            
            formatted_records.append({
                "node_id": node_id,
                "labels": label_str,
                "name": str(name),
                "properties": display_props[:3],  # Limit to 3 properties for display
                "relationships": relationship_strs[:3]  # Limit to 3 relationship types
            })
        
        # If we have regular data too, add it with special formatting
        if not formatted_records and graph_results.get("data"):
            # Fallback to regular data but try to extract graph elements
            for record in graph_results["data"][:50]:  # Limit to 50 records
                formatted_record = self._format_flat_record(record)
                if formatted_record:
                    formatted_records.append(formatted_record)
        
        return formatted_records
    
    def _build_relationship_index(self, relationships: List[Dict[str, Any]]) -> Dict[str, Dict[str, List]]:
        """
        Build an index of relationships by node ID.
        
        Args:
            relationships: List of relationship dictionaries
            
        Returns:
            Dictionary mapping node IDs to their relationships
        """
        index = defaultdict(lambda: {"outgoing": [], "incoming": []})
        
        for rel in relationships:
            start_node = rel.get("start")
            end_node = rel.get("end")
            
            if start_node:
                index[start_node]["outgoing"].append(rel)
            if end_node:
                index[end_node]["incoming"].append(rel)
        
        return index
    
    def _format_flat_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Format a flat record into graph display format.
        
        Args:
            record: Flat record dictionary
            
        Returns:
            Formatted record or None if not applicable
        """
        # Try to detect if this looks like graph data
        # Look for common graph-related fields
        
        # Determine the primary entity (state, farm, supplier, etc.)
        entity_name = None
        entity_type = None
        
        # Check for state data
        if "state" in record or "state_name" in record:
            entity_name = record.get("state") or record.get("state_name")
            entity_type = "State"
        elif "farm" in record or "farm_name" in record:
            entity_name = record.get("farm") or record.get("farm_name")
            entity_type = "Farm"
        elif "supplier" in record or "supplier_name" in record:
            entity_name = record.get("supplier") or record.get("supplier_name")
            entity_type = "Supplier"
        elif "name" in record:
            entity_name = record.get("name")
            entity_type = "Entity"
        
        if not entity_name:
            # Can't format without a primary entity
            return None
        
        # Extract properties (exclude the name field)
        properties = []
        relationships = []
        
        for key, value in record.items():
            if key in ["state", "state_name", "farm", "farm_name", "supplier", "supplier_name", "name"]:
                continue
            
            # Check if this looks like a relationship field
            if any(rel_word in key.lower() for rel_word in ["connected", "supplies", "located", "borders", "related"]):
                if value:
                    relationships.append(f"→{key.upper()}({value if isinstance(value, int) else 1})")
            else:
                # Regular property
                if value is not None:
                    if isinstance(value, (int, float)) and value >= 1000:
                        formatted_value = f"{value:,.0f}" if isinstance(value, int) else f"{value:,.2f}"
                    else:
                        formatted_value = str(value)
                    properties.append(f"{key}: {formatted_value}")
        
        return {
            "node_id": f"n:{hash(entity_name) % 10000}",  # Generate a pseudo ID
            "labels": f"[:{entity_type}]",
            "name": entity_name,
            "properties": properties[:3],
            "relationships": relationships[:3] if relationships else ["→MEASURED(1)"]
        }
    
    def _format_from_node_fields(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Format flat data with node fields into graph display format.
        
        Args:
            data: List of records with potential node fields
            
        Returns:
            List of formatted records for graph display
        """
        formatted_records = []
        
        for record in data[:50]:  # Limit to 50 records
            # Find node fields (fields ending with _node)
            node_fields = {k: v for k, v in record.items() if k.endswith("_node") and v is not None}
            
            if node_fields:
                # Use the first node field as primary entity
                primary_node_key = list(node_fields.keys())[0]
                primary_node = node_fields[primary_node_key]
                
                # Extract entity type from field name (e.g., "state_node" -> "State")
                entity_type = primary_node_key.replace("_node", "").capitalize()
                
                # Get the name from the corresponding field
                name_field = primary_node_key.replace("_node", "")
                entity_name = record.get(name_field, record.get("name", "Unknown"))
                
                # Build properties from other fields
                properties = []
                relationships = []
                
                for key, value in record.items():
                    if key.endswith("_node") or key == name_field:
                        continue
                    
                    if value is not None:
                        # Format value
                        if isinstance(value, (int, float)) and value >= 1000:
                            formatted_value = f"{value:,.0f}" if isinstance(value, int) else f"{value:,.2f}"
                        else:
                            formatted_value = str(value)
                        
                        # Check if this looks like a relationship
                        if any(rel_word in key.lower() for rel_word in ["connected", "related", "linked"]):
                            relationships.append(f"→{key.upper()}({formatted_value})")
                        else:
                            properties.append(f"{key}: {formatted_value}")
                
                # If we have other node fields, add them as relationships
                for other_node_key, other_node in node_fields.items():
                    if other_node_key != primary_node_key:
                        rel_type = other_node_key.replace("_node", "").upper()
                        relationships.append(f"→HAS_{rel_type}(1)")
                
                # Add meaningful relationships based on entity type
                if not relationships:
                    if entity_type.lower() == 'state':
                        # Add geographic relationships for states
                        geo_rels = self.relationship_builder.build_geographic_relationships(entity_name)
                        
                        # Group relationships by type
                        rel_groups = {}
                        for geo_rel in geo_rels:
                            rel_type = geo_rel['type']
                            if rel_type not in rel_groups:
                                rel_groups[rel_type] = []
                            rel_groups[rel_type].append(geo_rel.get('target', ''))
                        
                        # Format unique relationships
                        for rel_type, targets in rel_groups.items():
                            if rel_type == 'BORDERS':
                                unique_count = len(set(targets))
                                if unique_count > 0:
                                    relationships.append(f"→BORDERS({unique_count})")
                            elif rel_type in ['IN_CORN_BELT', 'IN_WHEAT_BELT', 'IN_COTTON_BELT']:
                                relationships.append(f"→{rel_type}(1)")
                            elif rel_type == 'IN_REGION':
                                if targets:
                                    relationships.append(f"→IN_{targets[0].upper()}(1)")
                        
                        relationships = relationships[:3]
                    elif entity_type.lower() == 'measurement':
                        # Add measurement type relationships
                        for prop in properties:
                            if 'Income' in prop:
                                relationships.append("←MEASURES_INCOME(1)")
                                break
                            elif 'Expense' in prop:
                                relationships.append("←MEASURES_EXPENSES(1)")
                                break
                        if not relationships:
                            relationships.append("←MEASURES(1)")
                    else:
                        relationships = ["→CONNECTED(1)"]
                
                formatted_records.append({
                    "node_id": f"n:{hash(str(primary_node)) % 100000}",
                    "labels": f"[:{entity_type}]",
                    "name": str(entity_name),
                    "properties": properties[:3],
                    "relationships": relationships[:3]
                })
            else:
                # No node fields, use the flat record formatter
                formatted_record = self._format_flat_record(record)
                if formatted_record:
                    formatted_records.append(formatted_record)
        
        return formatted_records
    
    def detect_graph_format(self, results: Dict[str, Any]) -> bool:
        """
        Detect if results contain graph structure data.
        
        Args:
            results: Query results dictionary
            
        Returns:
            True if graph structure is present
        """
        return "graph_structure" in results and bool(
            results["graph_structure"].get("nodes") or 
            results["graph_structure"].get("relationships")
        )