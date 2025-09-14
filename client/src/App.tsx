
import Menu from './components/Menu'
import Chat from './components/Chat'

function App() {

  return (
    <div className="h-screen flex">
      <div className="basis-[15%] shrink-0 grow-0 border-r border-gray-200">
        <Menu />
      </div>
      <div className="basis-[85%] grow-0 shrink-0">
        <Chat />
      </div>
    </div>
  )
}

export default App
