import React, { useEffect, useRef } from 'react';
import { Box, Typography } from '@mui/material';
import { useTypewriter } from '../hooks/useTypewriter';
import { StreamingTextProps } from '../types';
import { formatAsGraphTable } from '../utils/graphTableFormatter';

const StreamingText: React.FC<StreamingTextProps> = ({
  content,
  speed = 30,
  onComplete,
  className = '',
  displayFormat = 'table',
  rawData = null,
}) => {
  // Check if this is structured content (analysis + data table format)
  const isStructuredContent = content && (
    content.includes('=== ANALYSIS ===') || 
    content.includes('=== DATA TABLE ===')
  );
  
  // Always call useTypewriter (React Hooks rule)
  const typewriterResult = useTypewriter(content, { speed, onComplete });
  
  // For structured content, bypass typewriter animation by using content directly
  const { displayText, isTyping } = isStructuredContent 
    ? { displayText: content, isTyping: false }
    : typewriterResult;
  
  const containerRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom as text appears
  useEffect(() => {
    if (containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [displayText]);

  // Parse content into sections for different formatting
  const parseContentSections = (text: string) => {
    if (!text) return [];
    
    const sections = [];
    const lines = text.split('\n');
    let currentSection = { type: 'text', content: [] as string[] };
    let inTable = false;
    
    for (const line of lines) {
      // Check for section markers
      if (line === '=== ANALYSIS ===' || line === '=== ANÁLISIS ===') {
        // Save previous section if it has content
        if (currentSection.content.length > 0) {
          sections.push({
            ...currentSection,
            content: currentSection.content.join('\n')
          });
        }
        currentSection = { type: 'analysis', content: [line] };
        inTable = false;
      } else if (line === '=== DATA TABLE ===' || line === '=== TABLA DE DATOS ===' || line === '=== GRAPH DATA ===') {
        // Save previous section if it has content
        if (currentSection.content.length > 0) {
          sections.push({
            ...currentSection,
            content: currentSection.content.join('\n')
          });
        }
        // Check if this is graph data based on displayFormat
        const tableType = displayFormat === 'neo4j_graph' ? 'graph-table-header' : 'table-header';
        currentSection = { type: tableType, content: [line] };
        inTable = true;
      } else if (inTable && (line.includes('┌') || line.includes('│') || line.includes('└') || line.includes('├') || line.includes('┼') || line.includes('┤') || line.includes('╞') || line.includes('╪') || line.includes('╡') || line.includes('╔') || line.includes('║') || line.includes('╚'))) {
        // This is table content (including Neo4j double-line box characters)
        const tableType = displayFormat === 'neo4j_graph' ? 'graph-table' : 'table';
        if (currentSection.type !== tableType && currentSection.type !== 'table') {
          // Save previous section and start table section
          if (currentSection.content.length > 0) {
            sections.push({
              ...currentSection,
              content: currentSection.content.join('\n')
            });
          }
          currentSection = { type: tableType, content: [] };
        }
        currentSection.content.push(line);
      } else if (inTable && currentSection.type === 'table' && line.trim() && !line.includes('rows')) {
        // Continue table content
        currentSection.content.push(line);
      } else if (inTable && line.includes('rows')) {
        // Row count - add to table and end table section
        currentSection.content.push(line);
        sections.push({
          ...currentSection,
          content: currentSection.content.join('\n')
        });
        currentSection = { type: 'text', content: [] };
        inTable = false;
      } else {
        // Regular text or analysis content
        if (inTable && currentSection.type === 'table') {
          // End table section if we hit non-table content
          if (currentSection.content.length > 0) {
            sections.push({
              ...currentSection,
              content: currentSection.content.join('\n')
            });
          }
          currentSection = { type: 'analysis', content: [line] };
          inTable = false;
        } else {
          currentSection.content.push(line);
        }
      }
    }
    
    // Add the last section
    if (currentSection.content.length > 0) {
      sections.push({
        ...currentSection,
        content: currentSection.content.join('\n')
      });
    }
    
    return sections;
  };

  // Format the text to preserve line breaks and structure
  const formatText = (text: string) => {
    // Try to parse as JSON first for structured data
    try {
      const parsed = JSON.parse(text);
      return (
        <pre className="whitespace-pre-wrap font-mono text-sm">
          {JSON.stringify(parsed, null, 2)}
        </pre>
      );
    } catch {
      // Parse into sections for structured content
      const sections = parseContentSections(text);
      
      if (sections.length > 0) {
        return (
          <>
            {sections.map((section, index) => {
              if (section.type === 'analysis') {
                // Analysis content - constrained width for readability
                return (
                  <div key={index} className="analysis-section">
                    {section.content.split('\n').map((line, lineIdx) => (
                      <React.Fragment key={lineIdx}>
                        {line}
                        {lineIdx < section.content.split('\n').length - 1 && <br />}
                      </React.Fragment>
                    ))}
                  </div>
                );
              } else if (section.type === 'table' || section.type === 'graph-table') {
                // Check if this should be a graph table
                if ((section.type === 'graph-table' || displayFormat === 'neo4j_graph') && rawData) {
                  // Format as graph table
                  const graphTable = formatAsGraphTable(rawData, displayFormat);
                  return (
                    <pre key={index} className="ascii-table whitespace-pre font-mono text-xs leading-relaxed">
                      {graphTable}
                    </pre>
                  );
                } else {
                  // Regular table content - full width with horizontal scroll
                  return (
                    <pre key={index} className="ascii-table whitespace-pre font-mono text-xs leading-relaxed">
                      {section.content}
                    </pre>
                  );
                }
              } else if (section.type === 'table-header' || section.type === 'graph-table-header') {
                // Table header - styled like analysis
                return (
                  <div key={index} className="analysis-section font-semibold mt-4">
                    {section.content}
                  </div>
                );
              } else {
                // Regular text
                return (
                  <div key={index} className="analysis-section">
                    {section.content.split('\n').map((line, lineIdx) => (
                      <React.Fragment key={lineIdx}>
                        {line}
                        {lineIdx < section.content.split('\n').length - 1 && <br />}
                      </React.Fragment>
                    ))}
                  </div>
                );
              }
            })}
          </>
        );
      } else {
        // Fallback for simple text
        return (
          <div className="prose prose-sm max-w-none">
            {text.split('\n').map((line, index) => (
              <React.Fragment key={index}>
                {line}
                {index < text.split('\n').length - 1 && <br />}
              </React.Fragment>
            ))}
          </div>
        );
      }
    }
  };

  return (
    <Box 
      ref={containerRef}
      className={`relative ${className}`}
    >
      <Typography
        component="div"
        className="text-gray-800"
      >
        {formatText(displayText)}
        {isTyping && <span className="typing-cursor" />}
      </Typography>
    </Box>
  );
};

export default StreamingText;