import { useState, useEffect } from "react";

interface Session {
  session_id: string;
  user_id: string | null;
  summary: string | null;
  created_at: string;
  message_count: number;
  document_count: number;
}

interface HistoryResponse {
  sessions: Session[];
  total_sessions: number;
}

export default function Menu() {
  const [history, setHistory] = useState<HistoryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null);
  const [deletingSessionId, setDeletingSessionId] = useState<string | null>(null);

  useEffect(() => {
    const fetchHistory = async () => {
      try {
        setLoading(true);
        const response = await fetch("/history");
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        setHistory(data);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch history');
        console.error('Error fetching history:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchHistory();
  }, []);

  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const handleSessionClick = (sessionId: string) => {
    setSelectedSessionId(sessionId);
    // Dispatch custom event to communicate with Chat component
    const event = new CustomEvent('sessionSelected', { 
      detail: { sessionId } 
    });
    window.dispatchEvent(event);
  };

  const handleRefresh = () => {
    setLoading(true);
    fetch("/history")
      .then(res => res.json())
      .then(data => {
        setHistory(data);
        setError(null);
      })
      .catch(err => {
        setError(err instanceof Error ? err.message : 'Failed to fetch history');
        console.error('Error fetching history:', err);
      })
      .finally(() => setLoading(false));
  };

  const handleNewChat = () => {
    setSelectedSessionId(null);
    // Dispatch custom event to start new chat
    const event = new CustomEvent('newChat');
    window.dispatchEvent(event);
  };

  const handleDeleteSession = async (sessionId: string, event: React.MouseEvent) => {
    event.stopPropagation(); // Prevent session selection when clicking delete
    
    if (!confirm('Are you sure you want to delete this chat session? This action cannot be undone.')) {
      return;
    }

    setDeletingSessionId(sessionId);

    try {
      const response = await fetch(`/delete-session/${sessionId}`, {
        method: 'DELETE',
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      // Refresh the history list
      handleRefresh();
      
      // If the deleted session was selected, clear the selection
      if (selectedSessionId === sessionId) {
        setSelectedSessionId(null);
        const event = new CustomEvent('newChat');
        window.dispatchEvent(event);
      }
      
    } catch (error) {
      console.error('Error deleting session:', error);
      alert('Failed to delete session. Please try again.');
    } finally {
      setDeletingSessionId(null);
    }
  };

  return (
    <div className="h-full p-4 flex flex-col">
      <div className="mb-4">
        <div className="flex justify-between items-center">
          <div>
            <h2 className="text-2xl font-semibold text-gray-900">Bluejay AI</h2>
            <p className="text-sm text-gray-500">Chat History</p>
          </div>
          <div className="flex space-x-1">
            <button
              onClick={handleNewChat}
              className="p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-md transition-colors"
              title="Start new chat"
            >
              <svg 
                className="w-4 h-4" 
                fill="none" 
                stroke="currentColor" 
                viewBox="0 0 24 24"
              >
                <path 
                  strokeLinecap="round" 
                  strokeLinejoin="round" 
                  strokeWidth={2} 
                  d="M12 4v16m8-8H4" 
                />
              </svg>
            </button>
            <button
              onClick={handleRefresh}
              disabled={loading}
              className="p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-md transition-colors disabled:opacity-50"
              title="Refresh history"
            >
              <svg 
                className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} 
                fill="none" 
                stroke="currentColor" 
                viewBox="0 0 24 24"
              >
                <path 
                  strokeLinecap="round" 
                  strokeLinejoin="round" 
                  strokeWidth={2} 
                  d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" 
                />
              </svg>
            </button>
          </div>
        </div>
      </div>

      {loading && (
        <div className="flex items-center justify-center py-8">
          <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
          <span className="ml-2 text-sm text-gray-600">Loading...</span>
        </div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-md p-3 mb-4">
          <p className="text-sm text-red-600">Error: {error}</p>
          <button 
            onClick={() => window.location.reload()} 
            className="text-xs text-red-500 underline mt-1"
          >
            Retry
          </button>
        </div>
      )}

      {history && !loading && (
        <div className="flex-1 overflow-y-auto">
          {history.sessions.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-sm text-gray-500">No chat history yet</p>
              <p className="text-xs text-gray-400 mt-1">Start a conversation to see it here</p>
            </div>
          ) : (
            <div className="space-y-2">
              {history.sessions.map((session) => (
                <div
                  key={session.session_id}
                  onClick={() => handleSessionClick(session.session_id)}
                  className={`p-3 rounded-lg border cursor-pointer transition-colors ${
                    selectedSessionId === session.session_id
                      ? 'border-blue-500 bg-blue-100'
                      : 'border-gray-200 hover:border-blue-300 hover:bg-blue-50'
                  }`}
                >
                  <div className="flex justify-between items-start mb-2">
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-gray-800 truncate">
                        {session.summary || 'New Chat'}
                      </p>
                      <p className="text-xs text-gray-500">
                        {formatDate(session.created_at)}
                      </p>
                    </div>
                    <div className="flex items-center space-x-2 ml-2">
                      {session.message_count > 0 && (
                        <span className="inline-flex items-center px-2 py-1 rounded-full text-xs bg-blue-100 text-blue-800">
                          {session.message_count} msgs
                        </span>
                      )}
                      {session.document_count > 0 && (
                        <span className="inline-flex items-center px-2 py-1 rounded-full text-xs bg-green-100 text-green-800">
                          {session.document_count} docs
                        </span>
                      )}
                      <button
                        onClick={(e) => handleDeleteSession(session.session_id, e)}
                        disabled={deletingSessionId === session.session_id}
                        className={`p-1 rounded-md transition-colors ${
                          deletingSessionId === session.session_id
                            ? 'text-gray-300 cursor-not-allowed'
                            : 'text-gray-400 hover:text-red-500 hover:bg-red-50'
                        }`}
                        title="Delete session"
                      >
                        {deletingSessionId === session.session_id ? (
                          <div className="w-3 h-3 border border-gray-300 border-t-transparent rounded-full animate-spin"></div>
                        ) : (
                          <svg 
                            className="w-3 h-3" 
                            fill="none" 
                            stroke="currentColor" 
                            viewBox="0 0 24 24"
                          >
                            <path 
                              strokeLinecap="round" 
                              strokeLinejoin="round" 
                              strokeWidth={2} 
                              d="M6 18L18 6M6 6l12 12" 
                            />
                          </svg>
                        )}
                      </button>
                    </div>
                  </div>
                  
                  {session.user_id && (
                    <p className="text-xs text-gray-400 truncate">
                      User: {session.user_id}
                    </p>
                  )}
                </div>
              ))}
            </div>
          )}
          
          {history.total_sessions > 0 && (
            <div className="mt-4 pt-3 border-t border-gray-200">
              <p className="text-xs text-gray-500 text-center">
                {history.total_sessions} session{history.total_sessions !== 1 ? 's' : ''} total
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}


