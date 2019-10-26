declare module 'node-lmdb' {
  export class Cursor {
    constructor(txn: any, dby: any)
    close(): any
    del(): any
    getCurrentBinary(): any
    getCurrentBinaryUnsafe(): any
    getCurrentBoolean(): any
    getCurrentNumber(): any
    getCurrentString(): any
    getCurrentStringUnsafe(): any
    goToDup(): any
    goToDupRange(): any
    goToFirst(): any
    goToFirstDup(): any
    goToKey(key: string): any
    goToLast(): any
    goToLastDup(): any
    goToNext(): any
    goToNextDup(): any
    goToPrev(): any
    goToPrevDup(): any
    goToRange(): any
  }
  export class Env {
    beginTxn(): any
    close(): any
    info(): any
    open(opts: any): any
    openDbi(opts?: any): any
    resize(): any
    stat(): any
    sync(): any
  }
  export const path: string
  export const version: {
    major: number
    minor: number
    patch: number
    versionString: string
  }
}
