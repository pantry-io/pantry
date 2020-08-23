export const addMessage = (store, message) => {
  store.setState((state) => {
    state.statsMessages.unshift(message)
    // TODO: If the size of messages becomes too large, do a shift to remove first item.
  })
}
