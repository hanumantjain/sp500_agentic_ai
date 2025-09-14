import sendIcon from '../assets/send.svg'
import attachmentIcon from '../assets/attachment.svg'
import { useEffect, useRef, useState } from 'react'

export default function Chat() {
  const [input, setInput] = useState('')
  const [messages, setMessages] = useState<{ role: 'user' | 'assistant' | 'typing'; content: string }[]>([])
  const [selectedFiles, setSelectedFiles] = useState<File[]>([])
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const handleSend = async () => {
    const trimmed = input.trim()
    if (!trimmed && selectedFiles.length === 0) return
    
    // Append user message
    setMessages(prev => [...prev, { role: 'user', content: trimmed }])
    setInput('')
    
    // Clear selected files and reset textarea
    const filesToSend = [...selectedFiles]
    setSelectedFiles([])
    setTimeout(autoResize, 0)
    
    // Show typing indicator at the end
    setMessages(prev => [...prev, { role: 'typing', content: '' }])
    
    // Send to FastAPI server and replace typing with response or error
    const url = (import.meta as any).env?.VITE_API_URL || '/ask'
    try {
      // Use FormData to send both text and files
      const formData = new FormData()
      formData.append('question', trimmed)
      
      // Add files to FormData (server expects 'files' field name)
      filesToSend.forEach((file) => {
        formData.append('files', file)
      })
      
      const res = await fetch(url, {
        method: 'POST',
        body: formData, // Don't set Content-Type header, let browser set it for FormData
      })
      const data = await res.json().catch(async () => ({ reply: await res.text() }))
      const assistantText = res.ok ? (data.reply ?? JSON.stringify(data)) : (data.error || `Failed (status ${res.status}).`)
      setMessages(prev => {
        const firstTypingIndex = prev.findIndex(m => m.role === 'typing')
        if (firstTypingIndex === -1) return [...prev, { role: 'assistant', content: assistantText }]
        const next = [...prev]
        next.splice(firstTypingIndex, 1, { role: 'assistant', content: assistantText })
        return next
      })
    } catch (err: any) {
      const assistantText = err?.message ? `Error: ${err.message}` : 'Network error sending to webhook.'
      setMessages(prev => {
        const firstTypingIndex = prev.findIndex(m => m.role === 'typing')
        if (firstTypingIndex === -1) return [...prev, { role: 'assistant', content: assistantText }]
        const next = [...prev]
        next.splice(firstTypingIndex, 1, { role: 'assistant', content: assistantText })
        return next
      })
    }
  }

  const textAreaRef = useRef<HTMLTextAreaElement | null>(null)
  const autoResize = () => {
    const el = textAreaRef.current
    if (!el) return
    el.style.height = '0px'
    const computed = window.getComputedStyle(el)
    const lineHeight = parseFloat(computed.lineHeight || '0')
    const maxHeight = lineHeight * 3
    const newHeight = Math.min(el.scrollHeight, maxHeight)
    el.style.height = newHeight + 'px'
    el.style.overflowY = el.scrollHeight > maxHeight ? 'auto' : 'hidden'
  }

  useEffect(() => {
    autoResize()
  }, [])

  useEffect(() => {
    const el = scrollRef.current
    if (el) {
      el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' })
    }
  }, [messages])

  const handleFileSelect = () => {
    fileInputRef.current?.click()
  }

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || [])
    if (files.length === 0) return
    
    setSelectedFiles(prev => [...prev, ...files])
    
    // Reset file input
    if (e.target) {
      e.target.value = ''
    }
  }

  const removeFile = (index: number) => {
    setSelectedFiles(prev => prev.filter((_, i) => i !== index))
  }

  return (
    <div className="h-full p-4 flex flex-col">
      {/* Scrollable messages area */}
      <div ref={scrollRef} className="flex-1 rounded-lg border border-gray-200 p-3 pt-10 px-10 overflow-y-auto mb-4">
        <div className="space-y-3">
          {messages.length === 0 ? (
            <div className="h-full flex items-center justify-center">
              <p className="text-lg text-gray-600 text-center">Hello, What would you like to know?</p>
            </div>
          ) : (
            messages.map((msg, idx) => (
              <div
                key={idx}
                className={`flex items-start gap-2 ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
              >
                {msg.role === 'assistant' && (
                  <div className="max-w-[70%] break-words rounded-2xl bg-gray-100 text-gray-900 text-sm px-3 py-2 shadow-sm">
                    {msg.content}
                  </div>
                )}
                {msg.role === 'typing' && (
                  <div className="max-w-[70%] break-words rounded-2xl bg-gray-100 text-gray-900 text-sm px-3 py-2 shadow-sm">
                    <span className="inline-flex gap-1">
                      <span className="w-1.5 h-1.5 rounded-full bg-gray-400 animate-bounce [animation-delay:-0.2s]"></span>
                      <span className="w-1.5 h-1.5 rounded-full bg-gray-400 animate-bounce [animation-delay:0s]"></span>
                      <span className="w-1.5 h-1.5 rounded-full bg-gray-400 animate-bounce [animation-delay:0.2s]"></span>
                    </span>
                  </div>
                )}
                {msg.role === 'user' && (
                  <div className="max-w-[70%] break-words rounded-2xl bg-blue-600 text-white text-sm px-3 py-2 shadow-sm">
                    {msg.content}
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      </div>
      
      {/* Fixed textarea at bottom */}
      <div className="border border-gray-200 rounded-4xl p-2 px-4 flex items-center gap-2 w-2/3 mx-auto">
        {/* File attachment button */}
        <button 
          onClick={handleFileSelect}
          className="p-2 cursor-pointer text-gray-500 hover:text-gray-700" 
          aria-label="Attach files"
        >
          <img src={attachmentIcon} alt="Attach" className="h-5 w-5" />
        </button>
        
        {/* Hidden file input */}
        <input
          ref={fileInputRef}
          type="file"
          multiple
          className="hidden"
          onChange={handleFileChange}
          accept="image/*,.pdf,.doc,.docx,.txt,.csv,.xlsx"
        />
        
        {/* File chips inside textarea area */}
        {selectedFiles.map((file, index) => (
          <div key={index} className="flex items-center gap-1 bg-gray-100 px-2 py-1 rounded-md text-xs">
            <span className="text-gray-700 truncate max-w-[80px]">{file.name}</span>
            <button 
              onClick={() => removeFile(index)}
              className="text-gray-400 hover:text-red-500 text-xs ml-1"
            >
              ✕
            </button>
          </div>
        ))}
        
        <textarea
          ref={textAreaRef}
          rows={1}
          placeholder="Type a message..."
          className="flex-1 bg-transparent outline-none px-2 py-2 resize-none"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onInput={autoResize}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault()
              handleSend()
            }
          }}
        />
        <button className="p-2 cursor-pointer" aria-label="Send" onClick={() => {handleSend()}}>
          <img src={sendIcon} alt="Send" className="h-5 w-5" />
        </button>
      </div>
    </div>
  )
}


