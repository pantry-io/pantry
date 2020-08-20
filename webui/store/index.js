import React from 'react'
import Immer from 'immer'
import useGlobalHook from 'use-global-hook'
import * as actions from '../actions'
import { initialState } from './initialState'

// Add Immer lib to options to use Immer setState functions.
const options = {
  Immer,
}

const useGlobal = useGlobalHook(React, initialState, actions, options)

export default useGlobal
