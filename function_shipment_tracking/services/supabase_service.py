import requests
from typing import Dict, Any, Tuple, List
from dataclasses import dataclass
import time

from config import Config
from utils.logging_utils import setup_logger, log_structured

logger = setup_logger(__name__)

@dataclass
class SupabaseResult:
    """Result of Supabase Edge Function operation"""
    total: int
    successful: int
    failed: int
    errors: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_events': self.total,
            'successful': self.successful,
            'failed': self.failed,
            'errors': self.errors[:10]  # Limit errors returned
        }

class SupabaseService:
    """Handle Supabase Edge Function calls with retry logic"""
    
    def __init__(self):
        Config.validate()  # Ensure required config is present
        
        self.url = f"{Config.SUPABASE_URL}/functions/v1/process-shipment-tracking"
        self.headers = {
            'Authorization': f'Bearer {Config.SUPABASE_SERVICE_ROLE_KEY}',
            'Content-Type': 'application/json'
        }
        self.timeout = Config.SUPABASE_TIMEOUT
        self.max_retries = Config.MAX_RETRY_ATTEMPTS
        self.batch_size = Config.BATCH_SIZE
        
        # Log configuration for debugging (mask sensitive data)
        log_structured(logger, "SupabaseService initialized",
                      url=self.url,
                      timeout=self.timeout,
                      max_retries=self.max_retries,
                      batch_size=self.batch_size,
                      has_auth_token=bool(Config.SUPABASE_SERVICE_ROLE_KEY))
    
    def _send_batch_to_supabase(self, events: List[Dict[str, Any]], 
                               retry_count: int = 0) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Send batch of events to Supabase Edge Function with retry logic.
        
        Returns:
            Tuple of (success: bool, message: str, response_data: dict)
        """
        payload = {"events": events}
        
        # Add detailed request logging for debugging
        print(f"SENDING REQUEST TO: {self.url}")
        print(f"PAYLOAD SIZE: {len(events)} events")
        print(f"HEADERS: {dict(self.headers)}")
        
        log_structured(logger, "Sending request to Supabase Edge Function",
                      url=self.url,
                      headers={k: v[:20] + "..." if k == "Authorization" else v for k, v in self.headers.items()},
                      payload_size=len(events),
                      timeout=self.timeout)
        
        try:
            print(f"MAKING POST REQUEST...")
            response = requests.post(
                url=self.url,
                headers=self.headers,
                json=payload,
                timeout=self.timeout
            )
            print(f"REQUEST COMPLETED")
            
            # Use print for immediate visibility in Cloud Functions logs
            print(f"SUPABASE RESPONSE: Status={response.status_code}, Text={response.text[:500]}")
            
            log_structured(logger, "Supabase Edge Function response",
                         status_code=response.status_code,
                         events_sent=len(events),
                         response_text=response.text[:200])
            
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    print(f"SUCCESS: Got 200 response with data: {response_data}")
                    return True, f"Success: {response.status_code}", response_data
                except ValueError:
                    # Response is not JSON, but status is 200
                    print(f"SUCCESS: Got 200 response but no JSON data")
                    return True, f"Success: {response.status_code}", {}
            else:
                error_msg = f"Failed: {response.status_code} - {response.text[:200]}"
                print(f"ERROR: {error_msg}")
                log_structured(logger, "Supabase Edge Function error",
                             severity='ERROR',
                             status_code=response.status_code,
                             response_text=response.text[:200])
                
                # Retry on server errors
                if response.status_code >= 500 and retry_count < self.max_retries:
                    time.sleep(2 ** retry_count)  # Exponential backoff
                    return self._send_batch_to_supabase(events, retry_count + 1)
                
                return False, error_msg, {}
                
        except requests.exceptions.Timeout:
            error_msg = f"Timeout after {self.timeout}s"
            log_structured(logger, "Supabase request timeout",
                         severity='ERROR',
                         timeout=self.timeout,
                         events_count=len(events),
                         url=self.url)
            
            if retry_count < self.max_retries:
                time.sleep(2 ** retry_count)
                return self._send_batch_to_supabase(events, retry_count + 1)
            
            return False, error_msg, {}
            
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error: {str(e)}"
            log_structured(logger, "Supabase connection error",
                         severity='ERROR',
                         error=str(e),
                         url=self.url,
                         events_count=len(events))
            return False, error_msg, {}
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request exception: {str(e)}"
            log_structured(logger, "Supabase request exception",
                         severity='ERROR',
                         error=str(e),
                         url=self.url,
                         events_count=len(events))
            return False, error_msg, {}
            
        except Exception as e:
            error_msg = f"Unexpected exception: {str(e)}"
            log_structured(logger, "Supabase unexpected exception",
                         severity='ERROR',
                         error=str(e),
                         url=self.url,
                         events_count=len(events))
            return False, error_msg, {}
    
    def _chunk_events(self, events: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Split events into batches of specified size"""
        chunks = []
        for i in range(0, len(events), self.batch_size):
            chunks.append(events[i:i + self.batch_size])
        return chunks
    
    def test_connectivity(self) -> Tuple[bool, str]:
        """
        Test connectivity to Supabase Edge Function with a minimal request.
        This helps diagnose endpoint URL, authentication, and network issues.
        """
        test_payload = {"events": []}  # Empty events array should trigger validation error
        
        log_structured(logger, "Testing Supabase Edge Function connectivity",
                      url=self.url)
        
        try:
            response = requests.post(
                url=self.url,
                headers=self.headers,
                json=test_payload,
                timeout=10  # Short timeout for test
            )
            
            log_structured(logger, "Connectivity test response",
                          status_code=response.status_code,
                          response_text=response.text[:500])
            
            if response.status_code == 400:
                # Expected: validation error for empty events array
                return True, f"Connectivity OK - Got expected validation error (400): {response.text[:200]}"
            elif response.status_code == 401:
                return False, f"Authentication failed (401): {response.text[:200]}"
            elif response.status_code == 404:
                return False, f"Endpoint not found (404): {response.text[:200]}"
            else:
                return True, f"Connectivity OK - Got response ({response.status_code}): {response.text[:200]}"
                
        except requests.exceptions.ConnectionError as e:
            return False, f"Connection failed: {str(e)}"
        except requests.exceptions.Timeout:
            return False, "Request timed out"
        except Exception as e:
            return False, f"Test failed: {str(e)}"
    
    def send_tracking_events(self, events: List[Dict[str, Any]]) -> SupabaseResult:
        """
        Send tracking events to Supabase Edge Function in batches.
        
        Args:
            events: List of tracking event payload dictionaries
            
        Returns:
            SupabaseResult with summary statistics
        """
        if not events:
            logger.info("No events to send to Supabase")
            return SupabaseResult(total=0, successful=0, failed=0, errors=[])
        
        success_count = 0
        error_count = 0
        errors = []
        
        # Split into batches
        event_batches = self._chunk_events(events)
        
        log_structured(logger, "Starting Supabase batch processing",
                      total_events=len(events),
                      total_batches=len(event_batches),
                      batch_size=self.batch_size)
        
        for batch_index, batch in enumerate(event_batches):
            log_structured(logger, "Processing batch",
                         batch_index=batch_index + 1,
                         batch_size=len(batch))
            
            success, message, response_data = self._send_batch_to_supabase(batch)
            
            if success:
                # Parse Supabase Edge Function response for detailed stats
                if response_data:
                    batch_created = response_data.get('created', 0)
                    batch_updated = response_data.get('updated', 0)
                    batch_skipped = response_data.get('skipped', 0)
                    batch_errors = response_data.get('errors', 0)
                    
                    success_count += (batch_created + batch_updated)
                    error_count += batch_errors
                    
                    # Add any error messages from the response
                    if response_data.get('details', {}).get('error_messages'):
                        errors.extend(response_data['details']['error_messages'])
                    
                    log_structured(logger, "Batch processed successfully",
                                 batch_index=batch_index + 1,
                                 created=batch_created,
                                 updated=batch_updated,
                                 skipped=batch_skipped,
                                 errors=batch_errors)
                else:
                    # No detailed response, assume all events were successful
                    success_count += len(batch)
            else:
                error_count += len(batch)
                errors.append(f"Batch {batch_index + 1}: {message}")
        
        result = SupabaseResult(
            total=len(events),
            successful=success_count,
            failed=error_count,
            errors=errors
        )
        
        log_structured(logger, "Supabase batch processing completed",
                      **result.to_dict())
        
        return result