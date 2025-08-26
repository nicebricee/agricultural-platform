import { useState, useEffect, useRef } from 'react';

interface UseTypewriterOptions {
  speed?: number;
  onComplete?: () => void;
}

export const useTypewriter = (
  text: string,
  options: UseTypewriterOptions = {}
) => {
  const { speed = 30, onComplete } = options;
  // Ensure speed is positive and reasonable
  const safeSpeed = Math.max(1, Math.min(1000, speed || 30));
  const [displayText, setDisplayText] = useState('');
  const [isTyping, setIsTyping] = useState(false);
  const [currentIndex, setCurrentIndex] = useState(0);
  const intervalRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    if (!text) {
      setIsTyping(false);
      return;
    }

    // Edge case 1: No change - exact same text
    if (text === displayText) {
      // Text unchanged, skip processing regardless of typing position
      return;
    }

    // Edge case 2: Text got shorter (backend correction/truncation)
    const isShrinking = text.length < displayText.length;
    
    // Check if the new text is an extension of what's already displayed
    // Allow some typing lag tolerance for smoother streaming
    const TYPING_LAG_TOLERANCE = 10;
    const typingLag = displayText.length - currentIndex;
    const isTypingBehind = typingLag > TYPING_LAG_TOLERANCE;
    
    const isExtension = !isShrinking &&
                       displayText.length > 0 &&  // Don't treat empty as extension base
                       text.length > displayText.length && 
                       text.startsWith(displayText) && 
                       !isTypingBehind;  // Only reset if way behind
    
    if (isExtension) {
      // Continue typing from current position - don't reset or restart interval
      setIsTyping(true);
      // Keep existing displayText and currentIndex
      // Don't clear the interval - let it continue!
      return; // Early return to prevent starting duplicate interval
    } else {
      // Complete text change or shrinking - reset with optimization
      const MAX_INSTANT_CHARS = 10000;  // Increased to handle full AI responses
      
      if (text.length > MAX_INSTANT_CHARS) {
        // For very long initial text, show first portion instantly
        const instantPortion = text.substring(0, MAX_INSTANT_CHARS);
        setDisplayText(instantPortion);
        setCurrentIndex(MAX_INSTANT_CHARS);
      } else {
        // Normal reset for shorter text
        setDisplayText('');
        setCurrentIndex(0);
      }
      setIsTyping(true);
      
      // Only clear interval for non-extensions
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    }

    // Start typing animation (only if not already running for extensions)
    // Guard against React Strict Mode double-mounting
    if (!intervalRef.current) {
      intervalRef.current = setInterval(() => {
        setCurrentIndex((prevIndex) => {
          if (prevIndex < text.length) {
            setDisplayText(text.substring(0, prevIndex + 1));
            return prevIndex + 1;
          } else {
            // Typing complete
            if (intervalRef.current) {
              clearInterval(intervalRef.current);
              intervalRef.current = null;
            }
            setIsTyping(false);
            if (onComplete) {
              onComplete();
            }
            return prevIndex;
          }
        });
      }, safeSpeed);
    }

    // Cleanup function
    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [text, safeSpeed, onComplete, displayText, currentIndex]);

  // Force complete typing
  const skipTyping = () => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
    setDisplayText(text);
    setCurrentIndex(text.length);
    setIsTyping(false);
    if (onComplete) {
      onComplete();
    }
  };

  return {
    displayText,
    isTyping,
    skipTyping,
    progress: text.length > 0 ? currentIndex / text.length : 0,
  };
};