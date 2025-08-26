import axios, { AxiosInstance } from 'axios';
import { SearchRequest, SearchResponse } from '../types';

class ApiService {
  private client: AxiosInstance;

  constructor() {
    this.client = axios.create({
      baseURL: process.env.REACT_APP_API_URL || 'http://localhost:8000',
      timeout: 120000, // 2 minutes timeout for OpenAI processing
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Request interceptor
    this.client.interceptors.request.use(
      (config) => {
        // Add any auth headers here if needed
        return config;
      },
      (error) => {
        return Promise.reject(error);
      }
    );

    // Response interceptor
    this.client.interceptors.response.use(
      (response) => {
        return response;
      },
      (error) => {
        // Handle errors globally
        if (error.response) {
          // Server responded with error
          console.error('API Error:', error.response.data);
        } else if (error.request) {
          // Request made but no response
          console.error('Network Error:', error.message);
        } else {
          // Something else happened
          console.error('Error:', error.message);
        }
        return Promise.reject(error);
      }
    );
  }

  async search(request: SearchRequest): Promise<SearchResponse> {
    try {
      const response = await this.client.post<SearchResponse>(
        '/api/v1/search',
        request
      );
      console.log('API response received:', response.data);
      return response.data;
    } catch (error: any) {
      console.error('API error:', error);
      // Return error response in expected format
      // Note: backend doesn't send 'success' field, so we shouldn't add it
      throw error; // Let the App component handle the error
    }
  }

  async searchStream(
    request: SearchRequest,
    onMessage: (data: any) => void,
    onError?: (error: Error) => void,
    onComplete?: () => void
  ): Promise<AbortController> {
    const baseUrl = process.env.REACT_APP_API_URL || 'http://localhost:8000';
    const url = `${baseUrl}/api/v1/search/stream`;
    
    // Create AbortController for cancellation
    const abortController = new AbortController();
    
    // Send the request using fetch with streaming support
    fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: abortController.signal,
    }).then(response => {
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      // Process the stream
      const reader = response.body?.getReader();
      const decoder = new TextDecoder();
      
      const processStream = async () => {
        if (!reader) {
          throw new Error('No response body to stream');
        }
        
        try {
          let buffer = '';
          
          while (true) {
            const { done, value } = await reader.read();
            if (done) {
              if (onComplete) onComplete();
              break;
            }
            
            // Decode the chunk and add to buffer
            buffer += decoder.decode(value, { stream: true });
            
            // Process complete lines from buffer
            const lines = buffer.split('\n');
            // Keep the last incomplete line in the buffer
            buffer = lines.pop() || '';
            
            for (const line of lines) {
              if (line.startsWith('data: ')) {
                const data = line.slice(6);
                if (data.trim()) {
                  try {
                    const parsed = JSON.parse(data);
                    onMessage(parsed);
                    
                    // Check for completion
                    if (parsed.status === 'complete') {
                      if (onComplete) onComplete();
                      return;
                    }
                  } catch (e) {
                    console.error('Failed to parse SSE data:', e, 'Raw data:', data);
                  }
                }
              }
            }
          }
        } catch (error: any) {
          if (error.name === 'AbortError') {
            console.log('Stream aborted');
          } else {
            console.error('Stream processing error:', error);
            if (onError) onError(error);
          }
        } finally {
          reader.releaseLock();
        }
      };
      
      processStream();
    }).catch(error => {
      if (error.name !== 'AbortError') {
        console.error('Fetch error:', error);
        if (onError) onError(error);
      }
    });
    
    return abortController;
  }

  async checkHealth(): Promise<boolean> {
    try {
      const response = await this.client.get('/health');
      return response.data.status === 'healthy';
    } catch {
      return false;
    }
  }

  async getSystemInfo(): Promise<any> {
    try {
      const response = await this.client.get('/api/v1/system-info');
      return response.data;
    } catch (error) {
      console.error('Failed to get system info:', error);
      return null;
    }
  }
}

export default new ApiService();