/**
 * i18n configuration for Binance Trading Bot
 * - Default language: English
 * - Persist user's manual selection to localStorage
 * - Fallback to English for missing translations
 */

import i18n from 'i18next'
import { initReactI18next } from 'react-i18next'
import LanguageDetector from 'i18next-browser-languagedetector'

import en from './locales/en.json'

const resources = {
  en: { translation: en },
}

i18n
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    resources,
    fallbackLng: 'en',
    supportedLngs: ['en'],

    // Language detection options
    detection: {
      // Detection order: localStorage first, then browser language
      order: ['localStorage', 'navigator'],
      // Cache user's selection in localStorage
      caches: ['localStorage'],
      // localStorage key name
      lookupLocalStorage: 'arena-language'
    },

    interpolation: {
      escapeValue: false // React already escapes
    },

    // Return key as fallback (will show English from fallbackLng)
    returnEmptyString: false,

    // Don't load missing keys from backend
    saveMissing: false
  })

export default i18n
