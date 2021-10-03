// Copyright 2019-2020 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#ifndef O2_MCH_EVALUATION_MERGEABLE_COLLECTION_H
#define O2_MCH_EVALUATION_MERGEABLE_COLLECTION_H

///////////////////////////////////////////////////////////////////////////////
///
/// MergeableCollection
///
/// Collection of mergeable objects, indexed by key-tuples
///
/// Important point is that MergeableCollection is *always* the
/// owner of the objects it holds. This is why you should not
/// use the (inherited from TCollection) Add() method but the adopt() methods
///
/// \author Diego Stocco

#include "TString.h"
#include "TFolder.h"
#include "TIterator.h"
#include "TCollection.h"
#include <map>
#include <string>

class TMap;
class TH1;
class TH2;
class TProfile;
class THashList;
class TCollection;

namespace o2::mch::eval
{

class MergeableCollectionIterator;
class MergeableCollectionProxy;

class MergeableCollection : public TFolder
{
  friend class MergeableCollectionIterator; // our iterator class
  friend class MergeableCollectionProxy;    // out proxy class

 public:
  MergeableCollection(const char* name = "", const char* title = "");
  virtual ~MergeableCollection();

  virtual MergeableCollection* Clone(const char* name = "") const override;

  Bool_t attach(MergeableCollection* mc, const char* identifier, Bool_t pruneFirstIfAlreadyExists = kFALSE);

  Bool_t adopt(TObject* obj);
  Bool_t adopt(const char* identifier, TObject* obj);

  virtual void Browse(TBrowser* b) override;

  virtual void Clear(Option_t* option = "") override { Delete(option); }

  virtual TObject* FindObject(const char* fullIdentifier) const override;

  virtual TObject* FindObject(const TObject* object) const override;

  virtual void Delete(Option_t* option = "") override;

  virtual int numberOfObjects() const;

  virtual int numberOfKeys() const;

  TObject* getObject(const char* fullIdentifier) const;
  TObject* getObject(const char* identifier, const char* objectName) const;

  TH1* histo(const char* fullIdentifier) const;
  TH1* histo(const char* identifier, const char* objectName) const;

  TH1* h1(const char* fullIdentifier) const { return histo(fullIdentifier); }
  TH1* h1(const char* identifier, const char* objectName) const { return histo(identifier, objectName); }

  TH2* h2(const char* fullIdentifier) const;
  TH2* h2(const char* identifier, const char* objectName) const;

  TProfile* prof(const char* fullIdentifier) const;
  TProfile* prof(const char* identifier, const char* objectName) const;

  virtual MergeableCollectionProxy* createProxy(const char* identifier, Bool_t createIfNeeded = kFALSE);

  virtual TIterator* createIterator(Bool_t dir = kIterForward) const;

  virtual TList* createListOfKeys(Int_t index) const;

  virtual TList* createListOfObjectNames(const char* identifier) const;

  using TFolder::Remove;

  virtual TObject* remove(const char* fullIdentifier);

  Int_t removeByType(const char* typeName);

  TString getKey(const char* identifier, Int_t index, Bool_t idContainsObjName = kFALSE) const;
  TString getIdentifier(const char* fullIdentifier) const;
  TString getObjectName(const char* fullIdentifier) const;

  void Print(Option_t* option = "") const override;

  void clearMessages();
  void printMessages(const char* prefix = "") const;

  Long64_t Merge(TCollection* list);

  MergeableCollection* project(const char* identifier) const;

  UInt_t estimateSize(Bool_t show = kFALSE) const;

  /// Turn on the display of empty objects for the Print method
  void showEmptyObjects(Bool_t show = kTRUE)
  {
    fMustShowEmptyObject = show;
  }

  void pruneEmptyObjects();

  Int_t prune(const char* identifier);

  static Bool_t MergeObject(TObject* baseObject, TObject* objToAdd);

  TObject* getSum(const char* idPattern) const;

  Bool_t IsEmptyObject(TObject* obj) const;

  static void correctIdentifier(TString& sidentifier);

 private:
  MergeableCollection(const MergeableCollection& rhs);
  MergeableCollection& operator=(const MergeableCollection& rhs);

  TH1* histoWithAction(const char* identifier, TObject* o, const TString& action) const;

  Bool_t internalAdopt(const char* identifier, TObject* obj);

  TString internalDecode(const char* fullIdentifier, Int_t index) const;

  TObject* internalObject(const char* identifier, const char* objectName) const;

 public:
  TObjArray* sortAllIdentifiers() const;

  TString normalizeName(const char* identifier, const char* action) const;

  TMap* Map() const;

 private:
  mutable TMap* fMap;                           /// map of TMap of THashList* of TObject*...
  Bool_t fMustShowEmptyObject;                  /// Whether or not to show empty objects with the Print method
  mutable Int_t fMapVersion;                    /// internal version of map (to avoid custom streamer...)
  mutable std::map<std::string, int> fMessages; //! log messages

  ClassDefOverride(MergeableCollection, 1) /// A collection of mergeable objects
};

class MergeableCollectionIterator : public TIterator
{
 public:
  virtual ~MergeableCollectionIterator();

  MergeableCollectionIterator(const MergeableCollection* hcol, Bool_t direction = kIterForward);
  MergeableCollectionIterator& operator=(const TIterator& rhs);

  const TCollection* GetCollection() const { return 0x0; }

  TObject* Next();

  void Reset();

 private:
  const MergeableCollection* fkMergeableCollection; // Mergeable objects collection being iterated
  TIterator* fMapIterator;                          // Iterator for the internal map
  TIterator* fHashListIterator;                     // Iterator for the current hash list
  Bool_t fDirection;                                // forward or reverse

  MergeableCollectionIterator() : fkMergeableCollection(0x0), fMapIterator(0x0), fHashListIterator(0x0), fDirection(kIterForward) {}

  /// not implemented
  MergeableCollectionIterator& operator=(const MergeableCollectionIterator& rhs);
  /// not implemented
  MergeableCollectionIterator(const MergeableCollectionIterator& iter);

  ClassDef(MergeableCollectionIterator, 0) // Mergeable object collection iterator
};

class MergeableCollectionProxy : public TFolder
{
  friend class MergeableCollection;

 protected:
  MergeableCollectionProxy(MergeableCollection& oc, THashList& list);

 public:
  TObject* getObject(const char* objectName) const;

  TH1* histo(const char* objectName) const;

  TH1* h1(const char* objectName) const { return histo(objectName); }

  TH2* h2(const char* objectName) const;

  TProfile* prof(const char* objectName) const;

  void Print(Option_t* opt = "") const;

  Bool_t adopt(TObject* obj);

  Bool_t adopt(const char* identifier, TObject* obj);

  virtual TIterator* createIterator(Bool_t dir = kIterForward) const;

 private:
  MergeableCollection& fOC;
  THashList& fList;

  ClassDef(MergeableCollectionProxy, 0) // Mergeable object collection proxy
};

} // namespace o2::mch::eval
#endif
