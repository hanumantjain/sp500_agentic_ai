import sendIcon from '../assets/send.svg'
import { useEffect, useRef, useState } from 'react'

export default function Chat() {
  const [input, setInput] = useState('')
  const [messages, setMessages] = useState<{ role: 'user' | 'assistant' | 'typing'; content: string }[]>([])
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const handleSend = async () => {
    const trimmed = input.trim()
    if (!trimmed) return
    // Append user message
    setMessages(prev => [...prev, { role: 'user', content: trimmed }])
    setInput('')
    setTimeout(autoResize, 0)
    // Show typing indicator at the end
    setMessages(prev => [...prev, { role: 'typing', content: '' }])
    // Send to FastAPI server and replace typing with response or error
    const url = (import.meta as any).env?.VITE_API_URL || '/ask'
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: trimmed }),
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


