// If you return a function from an action it
// will be executed as a immer setState function
// export const add1 = (store) => (state) => {
//   state.cities[0].population[0].population++
// }

// import { loadWind } from "../services/loadWind";

// export const changeWeatherDescription = (store, event) => {
//   const description = event.target.value;
//   store.setState((state) => {
//     state.cities[0].weatherStatus.current.weather[0].description = description;
//   });
// };

// export const updateWind = async (store) => {
//   store.setState((state) => {
//     state.cities[0].weatherStatus.current.wind_speed = "Loading...";
//   });
//   const wind_speed = await loadWind();
//   store.setState((state) => {
//     state.cities[0].weatherStatus.current.wind_speed = wind_speed;
//   });
// };

export const setReadyState = (store, readyState) => {
  store.setState((state) => {
    state.websocket.readyState = readyState
    // TODO: If the size of messages becomes too large, do a shift to remove first item.
  })
}
