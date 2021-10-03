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

///
/// A mergeable object container (ported from AliRoot)
///
/// For each tuple (key1,key2,..,keyN) a (hash)list of mergeable objects is associated.
/// Note that key1, key2 (optional), ..., keyN (optional) are strings.
/// Those strings should not contain "/" themselves.
///
/// More helper functions might be added in the future (e.g. project, etc...)

#ifndef MERGEABLE_COLLECTION_STANDALONE
#include "Framework/Logger.h"
#include "MCHEvaluation/MergeableCollection.h"
#else
#include "MergeableCollection.h"
#endif
#include "Riostream.h"
#include "TBrowser.h"
#include "TError.h"
#include "TFolder.h"
#include "TGraph.h"
#include "TH1.h"
#include "TH2.h"
#include "THashList.h"
#include "THnSparse.h"
#include "TKey.h"
#include "TMap.h"
#include "TMethodCall.h"
#include "TObjArray.h"
#include "TObjString.h"
#include "TProfile.h"
#include "TROOT.h"
#include "TRegexp.h"
#include "TSystem.h"
#include <cassert>
#include <iostream>
#include <vector>

ClassImp(o2::mch::eval::MergeableCollection);
ClassImp(o2::mch::eval::MergeableCollectionProxy);
ClassImp(o2::mch::eval::MergeableCollectionIterator);

namespace o2::mch::eval
{

//_____________________________________________________________________________
MergeableCollection::MergeableCollection(const char* name, const char* title)
  : TFolder(name, title), fMap(0x0), fMustShowEmptyObject(0), fMapVersion(0), fMessages()
{
  /// Ctor
}

//_____________________________________________________________________________
MergeableCollection::~MergeableCollection()
{
  /// dtor. Note that the map is owner
  delete fMap;
}

//_____________________________________________________________________________
Bool_t
  MergeableCollection::adopt(TObject* obj)
{
  /// adopt a given object at top level (i.e. no key)
  return internalAdopt("", obj);
}

//_____________________________________________________________________________
void MergeableCollection::correctIdentifier(TString& sidentifier)
{
  /// Insure identifier has the right number of slashes...

  if (!sidentifier.IsNull()) {
    if (!sidentifier.EndsWith("/"))
      sidentifier.Append("/");
    if (!sidentifier.BeginsWith("/"))
      sidentifier.Prepend("/");
    sidentifier.ReplaceAll("//", "/");
  }
}

//_____________________________________________________________________________
Bool_t
  MergeableCollection::adopt(const char* identifier, TObject* obj)
{
  /// adopt a given object, and associate it with pair key
  TString sidentifier(identifier);

  correctIdentifier(sidentifier);

  return internalAdopt(sidentifier.Data(), obj);
}

//_____________________________________________________________________________
Bool_t MergeableCollection::attach(MergeableCollection* mc, const char* identifier, Bool_t pruneFirstIfAlreadyExists)
{
  /// attach an already existing mergeable collection to this one.
  /// It is attached at level identifier/
  /// We take ownership of mc
  /// If identifier is already existing we kill it if pruneFirstIfAlreadyExists is kTRUE
  /// (and attach mc) otherwise we return kFALSE (and do *not* attach mc)

  THashList* hlist = dynamic_cast<THashList*>(Map()->GetValue(identifier));

  if (hlist) {
    if (!pruneFirstIfAlreadyExists) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
      LOGP(error, "{} already exist. Will not overwrite it.", identifier);
#else
      Error("attach", "%s already exist. Will not overwirte it.", identifier);
#endif
      return kFALSE;
    } else {
      Int_t n = prune(identifier);
      if (!n) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
        LOGP(error, "Could not prune pre-existing {}", identifier);
#else
        Error("attach","Could not prune pre-existing %s", identifier);
#endif
        return kFALSE;
      }
    }
  }

  TIter next(mc->fMap);
  TObjString* str;

  while ((str = static_cast<TObjString*>(next()))) {
    THashList* hl = dynamic_cast<THashList*>(mc->Map()->GetValue(str->String()));
    TString newid(Form("/%s%s", identifier, str->String().Data()));
    newid.ReplaceAll("//", "/");
    Map()->Add(new TObjString(newid.Data()), hl);
  }

  return kTRUE;
}

//_____________________________________________________________________________
void MergeableCollection::Browse(TBrowser* b)
{
  /// Create a TFolder structure pointing to our objects, so we
  /// can be "browsed"

  if (!fFolders)
    return;

  TObjArray* ids = sortAllIdentifiers();
  TIter nextIdentifier(ids);
  TObjString* str;

  while ((str = static_cast<TObjString*>(nextIdentifier()))) {
    TObjArray* parts = str->String().Tokenize("/");
    TObjString* s;
    TIter nextPart(parts);
    TFolder* base = this;

    while ((s = static_cast<TObjString*>(nextPart()))) {
      TFolder* f = static_cast<TFolder*>(base->TFolder::FindObject(s->String()));
      if (!f) {
        f = new TFolder(s->String(), "");
        base->Add(f);
      }
      base = f;
    }
    delete parts;

    TList* list = createListOfObjectNames(str->String());
    if (list) {
      TObjString* oname;
      TIter nextObject(list);
      while ((oname = static_cast<TObjString*>(nextObject()))) {
        TObject* o = getObject(str->String(), oname->String());
        base->Add(o);
      }
    } else {
#ifndef MERGEABLE_COLLECTION_STANDALONE
      LOGP(error, "got list=0x0");
#endif
    }
    delete list;
  }

  TList* top = createListOfKeys(0);
  TObjString* stop;
  TIter nextTop(top);

  while ((stop = static_cast<TObjString*>(nextTop()))) {
    b->Add(TFolder::FindObject(stop->String()));
  }

  delete top;

  delete ids;
}

//_____________________________________________________________________________
void MergeableCollection::clearMessages()
{
  /// clear pending messages
  fMessages.clear();
}

//_____________________________________________________________________________
TIterator*
  MergeableCollection::createIterator(Bool_t direction) const
{
  /// Create an iterator (must be deleted by the client)
  return fMap ? new MergeableCollectionIterator(this, direction) : 0x0;
}

//_____________________________________________________________________________
MergeableCollectionProxy*
  MergeableCollection::createProxy(const char* identifier, Bool_t createIfNeeded)
{
  /// Create a proxy starting at identifier.
  /// If createIfNeeded is true, then the identifier is inserted into
  /// the collection if it does not exist yet (in which case this method always
  /// returns a non null proxy)

  TString sidentifier(identifier);
  correctIdentifier(sidentifier);

  THashList* list = static_cast<THashList*>(Map()->GetValue(sidentifier));
  if (!list) {
    if (!createIfNeeded) {
      return 0x0;
    }

    list = new THashList;
    list->SetOwner(kTRUE);

    Map()->Add(new TObjString(sidentifier), list);
    list->SetName(sidentifier);
  }
  return new MergeableCollectionProxy(*this, *list);
}

//_____________________________________________________________________________
MergeableCollection*
  MergeableCollection::Clone(const char* name) const
{
  /// Clone this collection.
  /// We loose the messages.

  MergeableCollection* newone = new MergeableCollection(name, GetTitle());

  newone->fMap = static_cast<TMap*>(fMap->Clone());
  newone->fMustShowEmptyObject = fMustShowEmptyObject;
  newone->fMapVersion = fMapVersion;

  return newone;
}

//_____________________________________________________________________________
void MergeableCollection::Delete(Option_t*)
{
  /// Delete all the objects
  if (fMap) {
    fMap->DeleteAll();
    delete fMap;
    fMap = 0x0;
  }
}

//_____________________________________________________________________________
TObject*
  MergeableCollection::FindObject(const char* fullIdentifier) const
{
  /// Find an object by its full identifier.

  return getObject(fullIdentifier);
}

//_____________________________________________________________________________
TObject*
  MergeableCollection::FindObject(const TObject* object) const
{
  /// Find an object
#ifndef MERGEABLE_COLLECTION_STANDALONE
  LOGP(warning, "This method is awfully inefficient. Please improve it or use FindObject(const char*)");
#endif
  TIter next(createIterator());
  TObject* obj;
  while ((obj = next())) {
    if (obj->IsEqual(object))
      return obj;
  }
  return 0x0;
}

//_____________________________________________________________________________
TList*
  MergeableCollection::createListOfKeys(Int_t index) const
{
  /// Create the list of keys at level index

  TList* list = new TList;
  list->SetOwner(kTRUE);

  TObjArray* ids = sortAllIdentifiers();
  TIter next(ids);
  TObjString* str;

  while ((str = static_cast<TObjString*>(next()))) {
    TString oneid = getKey(str->String().Data(), index, kFALSE);
    if (oneid.Length() > 0 && !list->Contains(oneid)) {
      list->Add(new TObjString(oneid));
    }
  }

  delete ids;
  return list;
}

//_____________________________________________________________________________
TList*
  MergeableCollection::createListOfObjectNames(const char* identifier) const
{
  /// Create list of object names for /key1/key2/key...
  /// Returned list must be deleted by client

  TList* listOfNames = new TList;
  listOfNames->SetOwner(kTRUE);

  TIter next(Map());
  TObjString* str;

  while ((str = static_cast<TObjString*>(next()))) {
    TString currIdentifier = str->String();
    if (currIdentifier.CompareTo(identifier))
      continue;

    THashList* list = static_cast<THashList*>(Map()->GetValue(identifier));

    TIter nextObject(list);
    TObject* obj;

    while ((obj = nextObject())) {
      listOfNames->Add(new TObjString(obj->GetName()));
    }
  }

  return listOfNames;
}

//_____________________________________________________________________________
TString
  MergeableCollection::getIdentifier(const char* fullIdentifier) const
{
  /// Extract the identifier from the fullIdentifier

  TString identifier;

  Int_t n = TString(fullIdentifier).CountChar('/') - 1;

  for (Int_t i = 0; i < n; ++i) {
    identifier += "/";
    identifier += internalDecode(fullIdentifier, i);
  }
  identifier += "/";
  return identifier;
}

//_____________________________________________________________________________
TString
  MergeableCollection::getKey(const char* identifier, Int_t index, Bool_t idContainsObjName) const
{
  /// Extract the index element of the key pair from the fullIdentifier

  if (!idContainsObjName) {
    TString sidentifier(identifier);
    sidentifier.Append("/dummy");
    return internalDecode(sidentifier.Data(), index);
  }

  return internalDecode(identifier, index);
}

//_____________________________________________________________________________
TString
  MergeableCollection::getObjectName(const char* fullIdentifier) const
{
  /// Extract the object name from an identifier

  return internalDecode(fullIdentifier, -1);
}

//_____________________________________________________________________________
TH1* MergeableCollection::histo(const char* fullIdentifier) const
{
  /// Get histogram key1/key2/.../objectName:action
  /// action is used for 2D histograms :
  /// might be px for Projection along x-axis
  /// py for Projection along y-axis
  /// pfx for profile along x-axis
  /// pfy for profile along y-axis

  TString sfullIdentifier(fullIdentifier);

  TString fullIdWithoutAction(fullIdentifier);
  TString action;

  if (sfullIdentifier.First(':') != kNPOS) {
    TObjArray* arr = sfullIdentifier.Tokenize(":");

    fullIdWithoutAction = static_cast<TObjString*>(arr->At(0))->String();

    if (arr->GetLast() > 0) {
      action = static_cast<TObjString*>(arr->At(1))->String();
      action.ToUpper();
    }

    delete arr;
  }

  Int_t nslashes = sfullIdentifier.CountChar('/');

  TObject* o(0x0);

  if (!nslashes) {
    o = getObject("", fullIdWithoutAction);
  } else {
    o = getObject(getIdentifier(fullIdWithoutAction).Data(), getObjectName(fullIdWithoutAction));
  }

  return histoWithAction(fullIdWithoutAction.Data(), o, action);
}

//_____________________________________________________________________________
TH1* MergeableCollection::histo(const char* identifier,
                                const char* objectName) const
{
  /// Get histogram key1/key2/.../objectName:action
  /// action is used for 2D histograms :
  /// might be px for Projection along x-axis
  /// py for Projection along y-axis
  /// pfx for profile along x-axis
  /// pfy for profile along y-axis

  TObject* o = getObject(identifier, objectName);

  TString action;

  if (strchr(objectName, ':')) {
    TObjArray* arr = TString(objectName).Tokenize(":");

    if (arr->GetLast() > 0) {
      action = static_cast<TObjString*>(arr->At(1))->String();
      action.ToUpper();
    }

    delete arr;

    return histoWithAction(identifier, o, action);
  }

  if (o && o->IsA()->InheritsFrom(TH1::Class())) {
    return static_cast<TH1*>(o);
  }
  return 0x0;
}

//_____________________________________________________________________________
TH1* MergeableCollection::histoWithAction(const char* identifier, TObject* o, const TString& action) const
{
  /// Convert o to an histogram if possible, applying a given action if there

  if (!o)
    return 0x0;

  if (!o->InheritsFrom("TH1")) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
    LOGP(error, "{} is not an histogram", o->GetName());
#endif
    return 0x0;
  }

  TH2* h2 = dynamic_cast<TH2*>(o);

  if (h2) {
    if (action == "PX") {
      return h2->ProjectionX(normalizeName(Form("%s/%s", identifier, o->GetName()), action.Data()).Data());
    }
    if (action == "PY") {
      return h2->ProjectionY(normalizeName(Form("%s/%s", identifier, o->GetName()), action.Data()).Data());
    }
    if (action == "PFX") {
      return h2->ProfileX(normalizeName(Form("%s/%s", identifier, o->GetName()), action.Data()).Data());
    }
    if (action == "PFY") {
      return h2->ProfileY(normalizeName(Form("%s/%s", identifier, o->GetName()), action.Data()).Data());
    }
  }

  return static_cast<TH1*>(o);
}

//_____________________________________________________________________________
TH2* MergeableCollection::h2(const char* fullIdentifier) const
{
  /// Short-cut method to grab a 2D histogram
  /// Will return 0x0 if the object if not a TH2xxx

  TObject* o = getObject(fullIdentifier);

  if (o->IsA()->InheritsFrom(TH2::Class())) {
    return static_cast<TH2*>(o);
  }
  return 0x0;
}

//_____________________________________________________________________________
TH2* MergeableCollection::h2(const char* identifier,
                             const char* objectName) const
{
  /// Short-cut method to grab a 2D histogram
  /// Will return 0x0 if the object if not a TH2xxx

  TObject* o = getObject(identifier, objectName);

  if (o->IsA()->InheritsFrom(TH2::Class())) {
    return static_cast<TH2*>(o);
  }
  return 0x0;
}

//_____________________________________________________________________________
TProfile*
  MergeableCollection::prof(const char* fullIdentifier) const
{
  /// Short-cut method to grab a TProfile histogram
  /// Will return 0x0 if the object if not a TProfile

  TObject* o = getObject(fullIdentifier);

  if (o->IsA()->InheritsFrom(TProfile::Class())) {
    return static_cast<TProfile*>(o);
  }
  return 0x0;
}

//_____________________________________________________________________________
TProfile*
  MergeableCollection::prof(const char* identifier,
                            const char* objectName) const
{
  /// Short-cut method to grab a TProfile histogram
  /// Will return 0x0 if the object if not a TProfile

  TObject* o = getObject(identifier, objectName);

  if (o->IsA()->InheritsFrom(TProfile::Class())) {
    return static_cast<TProfile*>(o);
  }
  return 0x0;
}

//_____________________________________________________________________________
TObject*
  MergeableCollection::getObject(const char* fullIdentifier) const
{
  /// Get object key1/key2/.../objectName
  /// Note that no action is allowed for generic objects (only for histograms,
  /// see histo() methods)

  TString sfullIdentifier(fullIdentifier);

  Int_t nslashes = sfullIdentifier.CountChar('/');

  if (!nslashes) {
    return getObject("", sfullIdentifier);
  } else {
    return getObject(getIdentifier(fullIdentifier).Data(), getObjectName(fullIdentifier));
  }
}

//_____________________________________________________________________________
TObject*
  MergeableCollection::getObject(const char* identifier,
                                 const char* objectName) const
{
  /// Get object for (identifier,objectName) triplet

  TString sidentifier(identifier);
  if (!sidentifier.IsNull()) {
    if (!sidentifier.BeginsWith("/"))
      sidentifier.Prepend("/");
    if (!sidentifier.EndsWith("/"))
      sidentifier.Append("/");
  }
  return internalObject(sidentifier.Data(), objectName);
}

//_____________________________________________________________________________
TObject* MergeableCollection::getSum(const char* idPattern) const
{
  /// Sum objects
  /// The pattern must be in the form:
  /// /key1_1,key1_2,.../key2_1,key2_2,.../.../objectName_1,objectName_2...
  /// The logical or between patterns separated by commas is taken
  /// Exact match is required for keys and objectNames

  TObject* sumObject = 0x0;
  TObjString* str = 0x0;

  // Build array of lists of pattern
  TString idPatternString(idPattern);
  TObjArray* keyList = idPatternString.Tokenize("/");
  TObjArray keyMatrix(keyList->GetEntriesFast());
  keyMatrix.SetOwner();
  TIter nextKey(keyList);
  while ((str = static_cast<TObjString*>(nextKey()))) {
    TObjArray* subKeyList = str->String().Tokenize(",");
    keyMatrix.Add(subKeyList);
  }
  delete keyList;

  TString debugMsg = "Adding objects:";

  //
  // First handle the keys
  //
  TObjString* subKey = 0x0;
  TIter next(Map());
  while ((str = static_cast<TObjString*>(next()))) {
    TString identifier = str->String();

    Bool_t listMatchPattern = kTRUE;
    for (Int_t ikey = 0; ikey < keyMatrix.GetEntries() - 1; ikey++) {
      TString currKey = getKey(identifier, ikey, kFALSE);
      Bool_t matchKey = kFALSE;
      TObjArray* subKeyList = static_cast<TObjArray*>(keyMatrix.At(ikey));
      TIter nextSubKey(subKeyList);
      while ((subKey = static_cast<TObjString*>(nextSubKey()))) {
        TString subKeyString = subKey->String();
        if (currKey == subKeyString) {
          matchKey = kTRUE;
          break;
        }
      } // loop on the list of patterns of each key
      if (!matchKey) {
        listMatchPattern = kFALSE;
        break;
      }
    } // loop on keys in the idPattern
    if (!listMatchPattern)
      continue;

    //
    // Then handle the object name
    //
    THashList* list = static_cast<THashList*>(Map()->GetValue(identifier.Data()));

    TIter nextObj(list);
    TObject* obj;

    while ((obj = nextObj())) {
      TString currKey = obj->GetName();
      Bool_t matchKey = kFALSE;
      TObjArray* subKeyList = static_cast<TObjArray*>(keyMatrix.Last());
      TIter nextSubKey(subKeyList);
      while ((subKey = static_cast<TObjString*>(nextSubKey()))) {
        TString subKeyString = subKey->String();
        if (currKey == subKeyString) {
          matchKey = kTRUE;
          break;
        }
      }
      if (!matchKey)
        continue;
      if (!sumObject)
        sumObject = obj->Clone();
      else
        MergeObject(sumObject, obj);
      debugMsg += Form(" %s%s", identifier.Data(), obj->GetName());
    } // loop on objects in list
  }   // loop on identifiers in map

#ifndef MERGEABLE_COLLECTION_STANDALONE
  LOGP(debug, debugMsg.Data());
#endif
  return sumObject;
}

//_____________________________________________________________________________
Bool_t MergeableCollection::internalAdopt(const char* identifier, TObject* obj)
{
  /// adopt an obj

  if (!obj) {
    Error("adopt", "Cannot adopt a null object");
    return kFALSE;
  }

  if (!obj->IsA()->InheritsFrom(TObject::Class()) ||
      !obj->IsA()->GetMethodWithPrototype("Merge", "TCollection*")) {
    Error("adopt", "Cannot adopt an object which is not mergeable!");
  }

  THashList* hlist = 0x0;

  hlist = static_cast<THashList*>(Map()->GetValue(identifier));

  if (!hlist) {
    hlist = new THashList;
    hlist->SetOwner(kTRUE);
    Map()->Add(new TObjString(identifier), hlist);
    hlist->SetName(identifier);
  }

  TObject* existingObj = hlist->FindObject(obj->GetName());

  if (existingObj) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
    LOGP(error, "Cannot adopt an already existing object : {} -> {}", identifier, existingObj->GetName());
#endif
    return kFALSE;
  }

  if (obj->IsA()->InheritsFrom(TH1::Class()))
    (static_cast<TH1*>(obj))->SetDirectory(0);

  hlist->AddLast(obj);

  return kTRUE;
}

//_____________________________________________________________________________
TString
  MergeableCollection::internalDecode(const char* identifier, Int_t index) const
{
  /// Extract the index-th element of the identifier (/key1/key2/.../keyN/objectName)
  /// object is index=-1 (i.e. last)

  if (strlen(identifier) > 0 && identifier[0] != '/') {
#ifndef MERGEABLE_COLLECTION_STANDALONE
    LOGP(error, "identifier {} is malformed (should start with /)", identifier);
#endif
    return "";
  }

  std::vector<Int_t> splitIndex;

  Int_t start(0);
  TString sidentifier(identifier);

  while (start < sidentifier.Length()) {
    Int_t pos = sidentifier.Index('/', start);
    if (pos == kNPOS)
      break;
    splitIndex.push_back(pos);
    start = pos + 1;
  }

  Int_t nkeys = splitIndex.size() - 1;

  if (index >= nkeys) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
    LOGP(error, "Requiring index {} of identifier {} which only have {}", index, identifier, nkeys);
#endif
    return "";
  }

  if (index < 0) {
    return sidentifier(splitIndex.back() + 1, sidentifier.Length() - splitIndex.back() - 1);
  }

  return sidentifier(splitIndex[index] + 1, splitIndex[index + 1] - splitIndex[index] - 1);
}

//_____________________________________________________________________________
TObject*
  MergeableCollection::internalObject(const char* identifier,
                                      const char* objectName) const
{
  /// Get object for (identifier,objectName)

  if (!fMap) {
    return 0x0;
  }

  THashList* hlist = static_cast<THashList*>(Map()->GetValue(identifier));
  if (!hlist) {
    TString msg(Form("Did not find hashlist for identifier=%s dir=%s", identifier, gDirectory ? gDirectory->GetName() : ""));
    fMessages[msg.Data()]++;
    return 0x0;
  }

  TObject* obj = hlist->FindObject(objectName);
  if (!obj) {
    TString msg(Form("Did not find objectName=%s in %s", objectName, identifier));
    fMessages[msg.Data()]++;
  }
  return obj;
}

//_____________________________________________________________________________
Bool_t MergeableCollection::IsEmptyObject(TObject* obj) const
{
  /// Check if object is empty
  /// (done only for TH1, so far)

  if (obj->IsA()->InheritsFrom(TH1::Class())) {
    TH1* histo = static_cast<TH1*>(obj);
    if (histo->GetEntries() == 0)
      return kTRUE;
  }

  return kFALSE;
}

//_____________________________________________________________________________
TMap* MergeableCollection::Map() const
{
  /// Wrapper to insure proper key formats (i.e. new vs old)

  if (!fMap) {
    fMap = new TMap;
    fMap->SetOwnerKeyValue(kTRUE, kTRUE);
    fMapVersion = 1;
  } else {
    if (fMapVersion < 1) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
      LOGP(info, "Remapping");
#endif
      // change the keys
      TIter next(fMap);
      TObjString* str;

      while ((str = static_cast<TObjString*>(next()))) {
        if (str->String().Contains("./")) {
          TString newkey(str->String());

          newkey.ReplaceAll("./", "");

          TObject* o = fMap->GetValue(str);

          TPair* p = fMap->RemoveEntry(str);
          if (!p) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
            LOGP(error, "oups oups oups");
#endif
            return 0x0;
          }

          fMap->Add(new TObjString(newkey.Data()), o);

          delete p;
        }
      }

      fMapVersion = 1;
    }
  }

  return fMap;
}

//_____________________________________________________________________________
Long64_t
  MergeableCollection::Merge(TCollection* list)
{
  // Merge a list of MergeableCollection objects with this
  // Returns the number of merged objects (including this).

  if (!list)
    return 0;

  if (list->IsEmpty())
    return 1;

  TIter next(list);
  TObject* currObj;
  TList mapList;
  Long64_t count(0);

  while ((currObj = next())) {
    MergeableCollection* mergeCol = dynamic_cast<MergeableCollection*>(currObj);
    if (!mergeCol) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
      LOGP(fatal, "object named \"{}\" is a {} instead of an MergeableCollection!", currObj->GetName(), currObj->ClassName());
#endif
      continue;
    }

    ++count;

    if (mergeCol->fMap)
      mergeCol->Map(); // to insure keys in the new format

    TIter nextIdentifier(mergeCol->fMap);
    TObjString* identifier;

    while ((identifier = static_cast<TObjString*>(nextIdentifier()))) {
      THashList* otherList = static_cast<THashList*>(mergeCol->fMap->GetValue(identifier->String().Data()));

      TIter nextObject(otherList);
      TObject* obj;

      while ((obj = nextObject())) {
        TString newid(Form("%s%s", identifier->String().Data(), obj->GetName()));

        TObject* thisObject = getObject(newid.Data());

        if (!thisObject) {
          Bool_t ok = adopt(identifier->String(), obj->Clone());

          if (!ok) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
            LOGP(error, "adoption of object {} failed", obj->GetName());
#endif
          }
        } else {
          // add it...
          MergeObject(thisObject, obj);
        }
      } // loop on objects in map
    }   // loop on identifiers
  }     // loop on collections in list

  return count + 1;
}

//_____________________________________________________________________________
Bool_t MergeableCollection::MergeObject(TObject* baseObject, TObject* objToAdd)
{
  /// Add objToAdd to baseObject

  if (baseObject->IsA()->Class() != objToAdd->IsA()->Class()) {
    printf("MergeObject: Cannot add %s to %s", objToAdd->ClassName(), baseObject->ClassName());
    return kFALSE;
  }
  if (!baseObject->IsA()->InheritsFrom(TObject::Class()) ||
      !baseObject->IsA()->GetMethodWithPrototype("Merge", "TCollection*")) {
    printf("MergeObject: Objects are not mergeable!");
    return kFALSE;
  }

  TList list;
  list.Add(objToAdd);

  TMethodCall callEnv;
  callEnv.InitWithPrototype(baseObject->IsA(), "Merge", "TCollection*");
  callEnv.SetParam((Long_t)(&list));
  callEnv.Execute(baseObject);
  return kTRUE;
}

//_____________________________________________________________________________
TString MergeableCollection::normalizeName(const char* identifier, const char* action) const
{
  /// Replace / by _ to build a root-compliant histo name
  TString name(GetName());

  name += "_";
  name += identifier;
  name += "_";
  name += action;
  name.ReplaceAll("/", "_");
  name.ReplaceAll("-", "_");
  return name;
}

//_____________________________________________________________________________
Int_t MergeableCollection::numberOfObjects() const
{
  /// Get the number of objects we hold
  TIter next(createIterator());
  Int_t count(0);
  while (next())
    ++count;
  return count;
}

//_____________________________________________________________________________
Int_t MergeableCollection::numberOfKeys() const
{
  /// Get the number of keys we have
  return fMap ? fMap->GetSize() : 0;
}

//_____________________________________________________________________________
void MergeableCollection::Print(Option_t* option) const
{
  /// Print all the objects we hold, in a hopefully visually pleasing
  /// way.
  ///
  /// Option can be used to select given part only, using the schema :
  /// /*/*/*/*/*
  /// Where the stars are wilcards for /key1/key2/.../objectName
  ///
  /// if * is used it is assumed to be a wildcard for objectName
  ///
  /// For other selections the full syntax /*/*/*/*/* must be used.
  ///
  /// Use "-" as objectName to disable object's name output
  ///
  /// One might also use /*/*/*/*/:classname syntax to restrict
  /// output to only those objects matching a given classname pattern
  ///

  std::cout << Form("MergeableCollection(%s,%s)[%p] : %d keys and %d objects\n",
                    GetName(), GetTitle(), this,
                    numberOfKeys(), numberOfObjects());

  if (!strlen(option))
    return;

  TString soption(option);

  TObjArray* classes = soption.Tokenize(":");

  TRegexp* classPattern(0x0);

  if (classes->GetLast() > 0) {
    TString pat = static_cast<TObjString*>(classes->At(1))->String();
    classPattern = new TRegexp(pat, kTRUE);
    soption = static_cast<TObjString*>(classes->At(0))->String();
  }

  delete classes;

  TObjArray* select = soption.Tokenize("/");

  TString sreObjectName(select->Last()->GetName());
  TRegexp reObjectName(sreObjectName.Data(), kTRUE);

  TObjArray* identifiers = sortAllIdentifiers();

  std::cout << Form("Number of identifiers %d\n", identifiers->GetEntries());

  TIter nextIdentifier(identifiers);

  TObjString* sid(0x0);

  while ((sid = static_cast<TObjString*>(nextIdentifier()))) {
    Bool_t identifierPrinted(kFALSE);

    TString identifier(sid->String());

    Bool_t matchPattern = kTRUE;
    for (Int_t isel = 0; isel < select->GetLast(); isel++) {
      if (!getKey(identifier.Data(), isel, kFALSE).Contains(TRegexp(select->At(isel)->GetName(), kTRUE))) {
        matchPattern = kFALSE;
        break;
      }
    }
    if (!matchPattern)
      continue;

    if (sreObjectName == "*" && !classPattern) {
      identifierPrinted = kTRUE;
      std::cout << identifier.Data() << "\n";
    }

    THashList* list = static_cast<THashList*>(Map()->GetValue(sid->String().Data()));

    TObjArray names;
    names.SetOwner(kTRUE);
    TIter nextUnsortedObj(list);
    TObject* obj;
    while ((obj = nextUnsortedObj())) {
      TString cname(obj->ClassName());
      if (classPattern && !cname.Contains((*classPattern))) {
        continue;
      }
      names.Add(new TObjString(obj->GetName()));
    }
    names.Sort();
    TIter nextObjName(&names);
    TObjString* oname;
    while ((oname = static_cast<TObjString*>(nextObjName()))) {
      TString objName(oname->String());
      if (objName.Contains(reObjectName)) {
        obj = list->FindObject(objName.Data());
        if (IsEmptyObject(obj) && !fMustShowEmptyObject)
          continue;

        if (!identifierPrinted) {
          std::cout << identifier.Data() << "\n";
          identifierPrinted = kTRUE;
        }

        TString extra;
        TString warning("   ");

        if (obj->IsA()->InheritsFrom(TH1::Class())) {

          TH1* histo = static_cast<TH1*>(obj);
          extra.Form("%s | Entries=%d Sum=%g", histo->GetTitle(), Int_t(histo->GetEntries()), histo->GetSumOfWeights());
        } else if (obj->IsA()->InheritsFrom(TGraph::Class())) {
          TGraph* graph = static_cast<TGraph*>(obj);
          if (!TMath::Finite(graph->GetMean(2))) {
            warning = " ! ";
          }
          extra.Form("%s | Npts=%d Mean=%g RMS=%g", graph->GetTitle(), graph->GetN(),
                     graph->GetMean(2), graph->GetRMS(2));
        }

        std::cout << Form("    (%s) %s %s", obj->ClassName(),
                          warning.Data(),
                          obj->GetName());

        if (extra.Length()) {
          std::cout << " | " << extra.Data();
        }
        std::cout << "\n";
      }
    }
    if (!identifierPrinted && sreObjectName == "-") {
      // to handle the case where we used objectName="-" to disable showing the objectNames,
      // but we still want to see the matching keys maybe...
      std::cout << identifier.Data() << "\n";
    }
  }

  delete select;

  delete identifiers;
}

//_____________________________________________________________________________
void MergeableCollection::printMessages(const char* prefix) const
{
  /// Print pending messages

  std::map<std::string, int>::const_iterator it;

  for (it = fMessages.begin(); it != fMessages.end(); ++it) {
    std::cout << Form("%s : message %s appeared %5d times\n", prefix, it->first.c_str(), it->second);
  }
}

//_____________________________________________________________________________
UInt_t
  MergeableCollection::estimateSize(Bool_t show) const
{
  /// estimate the memory (in kilobytes) used by some objects

  //  For TH1:
  //  sizeof(TH1) + (nbins+2)*(nbytes_per_bin) +name+title_sizes
  //  if you have errors add (nbins+2)*8

  TIter next(createIterator());

  TObject* obj;
  UInt_t size(0);

  while ((obj = next())) {
    UInt_t thissize = 0;
    if (obj->IsA()->InheritsFrom(TH1::Class()) || obj->IsA()->InheritsFrom(TProfile::Class())) {
      TH1* histo = static_cast<TH1*>(obj);
      Int_t nbins = (histo->GetNbinsX() + 2);

      if (histo->GetNbinsY() > 1) {
        nbins *= (histo->GetNbinsY() + 2);
      }

      if (histo->GetNbinsZ() > 1) {
        nbins *= (histo->GetNbinsZ() + 2);
      }

      Bool_t hasErrors = (histo->GetSumw2N() > 0);

      TString cname(histo->ClassName());

      Int_t nbytesPerBin(0);

      if (cname.Contains(TRegexp("C$")))
        nbytesPerBin = sizeof(Char_t);
      if (cname.Contains(TRegexp("S$")))
        nbytesPerBin = sizeof(Short_t);
      if (cname.Contains(TRegexp("I$")))
        nbytesPerBin = sizeof(Int_t);
      if (cname.Contains(TRegexp("F$")))
        nbytesPerBin = sizeof(Float_t);
      if (cname.Contains(TRegexp("D$")))
        nbytesPerBin = sizeof(Double_t);
      if (cname == "TProfile")
        nbytesPerBin = sizeof(Double_t);

      if (!nbytesPerBin) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
        LOGP(error, "Could not get the number of bytes per bin for histo {} of class {}. Thus the size estimate will be wrong !",
             histo->GetName(), histo->ClassName());
#endif
        continue;
      }

      thissize = sizeof(histo) + nbins * (nbytesPerBin) + strlen(histo->GetName()) + strlen(histo->GetTitle());

      if (hasErrors)
        thissize += nbins * 8;

      if (obj->IsA()->InheritsFrom(TProfile::Class())) {
        TProfile* prof = static_cast<TProfile*>(obj);
        TArrayD* d = prof->GetBinSumw2();
        thissize += d->GetSize() * 8 * 2; // 2 TArrayD
        thissize += sizeof(prof) - sizeof(histo);
      }
    } else if (obj->IsA()->InheritsFrom(THnSparse::Class())) {
      THnSparse* sparse = static_cast<THnSparse*>(obj);
      thissize = sizeof(Float_t) * (UInt_t)sparse->GetNbins();
    } else {
#ifndef MERGEABLE_COLLECTION_STANDALONE
      LOGP(warning, "Cannot estimate size of {}", obj->ClassName());
#endif
      continue;
    }

    size += thissize;

    if (show) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
      LOGP(info, "Size of {:30s} is {:20d} bytes", obj->GetName(), thissize);
#endif
    }
  } // loop on objects

  return size;
}

//_____________________________________________________________________________
Int_t MergeableCollection::prune(const char* identifier)
{
  // Delete all objects which match the beginning of the identifier
  // returns the number of entries removed from the Map()
  // (not to be confused with the number of leaf objects removed)
  //

  TIter next(Map());
  TObjString* key;
  Int_t ndeleted(0);

  while ((key = static_cast<TObjString*>(next()))) {
    if (key->String().BeginsWith(identifier)) {
      Bool_t ok = Map()->DeleteEntry(key);
      if (ok)
        ++ndeleted;
    }
  }

  return ndeleted;
}

//_____________________________________________________________________________
void MergeableCollection::pruneEmptyObjects()
{
  /// Delete the empty objects
  /// (Implemented for TH1 only)
  TIter next(Map());
  TObjString* key;

  TList toBeRemoved;
  toBeRemoved.SetOwner(kTRUE);

  while ((key = static_cast<TObjString*>(next()))) {
    TString identifier(key->String());
    THashList* hlist = static_cast<THashList*>(Map()->GetValue(identifier.Data()));
    TIter nextObject(hlist);
    TObject* obj;
    while ((obj = nextObject())) {
      if (IsEmptyObject(obj))
        toBeRemoved.Add(new TObjString(Form("%s%s", identifier.Data(), obj->GetName())));
    }
  }

  TIter nextTBR(&toBeRemoved);
  while ((key = static_cast<TObjString*>(nextTBR()))) {
    remove(key->GetString().Data());
#ifndef MERGEABLE_COLLECTION_STANDALONE
    LOGP(debug, "Removing {}", key->GetString().Data());
#endif
  }
}

//_____________________________________________________________________________
MergeableCollection*
  MergeableCollection::project(const char* identifier) const
{
  /// To be implemented : would create a new collection starting at /key1/key2/...

  if (!fMap)
    return 0x0;

  MergeableCollection* mergCol = new MergeableCollection(Form("%s %s", GetName(), identifier),
                                                         GetTitle());

  TIter next(Map());
  TObjString* str;

  while ((str = static_cast<TObjString*>(next()))) {
    TString currIdentifier = str->String();
    if (!currIdentifier.Contains(identifier))
      continue;

    THashList* list = static_cast<THashList*>(Map()->GetValue(identifier));

    TIter nextObj(list);
    TObject* obj;

    while ((obj = nextObj())) {
      TObject* clone = obj->Clone();

      TString newkey(currIdentifier.Data());
      newkey.ReplaceAll(identifier, "");

      if (newkey == "/")
        newkey = "";

      mergCol->internalAdopt(newkey.Data(), clone);
    }
  }

  return mergCol;
}

//_____________________________________________________________________________
TObject*
  MergeableCollection::remove(const char* fullIdentifier)
{
  ///
  /// Remove a given object (given its fullIdentifier=/key1/key2/.../objectName)
  ///
  /// Note that we do *not* remove the /key1/key2/... entry even if there's no
  /// more object for this triplet.
  ///
  /// Not very efficient. Could be improved ?
  ///

  TString identifier = getIdentifier(fullIdentifier);

  THashList* hlist = dynamic_cast<THashList*>(Map()->GetValue(identifier.Data()));

  if (!hlist) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
    LOGP(warning, "Could not get hlist for key={}", identifier.Data());
#endif
    return 0x0;
  }

  TObject* obj = getObject(fullIdentifier);
  if (!obj) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
    LOGP(error, "Could not find object {}", fullIdentifier);
#endif
    return 0x0;
  }

  TObject* rmObj = hlist->Remove(obj);
  if (!rmObj) {
#ifndef MERGEABLE_COLLECTION_STANDALONE
    LOGP(error, "Remove failed");
#endif
    return 0x0;
  }

  return rmObj;
}

//_____________________________________________________________________________
Int_t MergeableCollection::removeByType(const char* typeName)
{
  /// Remove all the objects in this collection that are of a given type
  TIter nextIdentifier(Map());
  TObjString* identifier;
  Int_t nremoved(0);

  while ((identifier = static_cast<TObjString*>(nextIdentifier()))) {
    THashList* list = static_cast<THashList*>(Map()->GetValue(identifier->String()));
    TIter next(list);
    TObject* o;

    while ((o = next())) {
      if (strcmp(o->ClassName(), typeName) == 0) {
        list->Remove(o);
        ++nremoved;
      }
    }
  }
  return nremoved;
}

//_____________________________________________________________________________
TObjArray*
  MergeableCollection::sortAllIdentifiers() const
{
  /// Sort our internal identifiers. Returned array must be deleted.
  TObjArray* identifiers = new TObjArray;
  identifiers->SetOwner(kFALSE);
  TIter next(Map());
  TObjString* sid;

  while ((sid = static_cast<TObjString*>(next()))) {
    if (!identifiers->FindObject(sid->String().Data())) {
      identifiers->Add(sid);
    }
  }
  identifiers->Sort();
  return identifiers;
}

///////////////////////////////////////////////////////////////////////////////
//
// MergeableCollectionIterator
//
///////////////////////////////////////////////////////////////////////////////

class MergeableCollectionIterator;

//_____________________________________________________________________________
MergeableCollectionIterator::MergeableCollectionIterator(const MergeableCollection* mcol, Bool_t dir)
  : fkMergeableCollection(mcol), fMapIterator(0x0), fHashListIterator(0x0), fDirection(dir)
{
  /// Default ctor
}

//_____________________________________________________________________________
MergeableCollectionIterator&
  MergeableCollectionIterator::operator=(const TIterator&)
{
  /// Overriden operator= (imposed by Root's declaration of TIterator ?)
  Fatal("TIterator::operator=", "Not implementeable"); // because there's no clone in TIterator :-(
  return *this;
}

//_____________________________________________________________________________
MergeableCollectionIterator::~MergeableCollectionIterator()
{
  /// dtor
  Reset();
}

//_____________________________________________________________________________
TObject* MergeableCollectionIterator::Next()
{
  /// Advance to next object in the collection

  if (!fHashListIterator) {
    if (!fMapIterator) {
      fMapIterator = fkMergeableCollection->fMap->MakeIterator(fDirection);
    }
    TObjString* key = static_cast<TObjString*>(fMapIterator->Next());
    if (!key) {
      // we are done
      return 0x0;
    }
    THashList* list = static_cast<THashList*>(fkMergeableCollection->Map()->GetValue(key->String().Data()));
    if (!list)
      return 0x0;
    fHashListIterator = list->MakeIterator(fDirection);
  }

  TObject* obj = fHashListIterator->Next();

  if (!obj) {
    delete fHashListIterator;
    fHashListIterator = 0x0;
    return Next();
  }

  return obj;
}

//_____________________________________________________________________________
void MergeableCollectionIterator::Reset()
{
  /// Reset the iterator
  delete fHashListIterator;
  delete fMapIterator;
}

///////////////////////////////////////////////////////////////////////////////
//
// MergeableCollectionProxy
//
///////////////////////////////////////////////////////////////////////////////

class MergeableCollectionProxy;

//_____________________________________________________________________________
MergeableCollectionProxy::MergeableCollectionProxy(MergeableCollection& oc,
                                                   THashList& list)
  : fOC(oc), fList(list)
{
  fName = fList.GetName();
}

//_____________________________________________________________________________
Bool_t MergeableCollectionProxy::adopt(TObject* obj)
{
  return fOC.adopt(fList.GetName(), obj);
}

//_____________________________________________________________________________
Bool_t
  MergeableCollectionProxy::adopt(const char* identifier, TObject* obj)
{
  /// adopt a given object, and associate it with pair key

  TString path;
  path.Form("%s%s", fList.GetName(), identifier);
  return fOC.adopt(path, obj);
}

//_____________________________________________________________________________
TObject* MergeableCollectionProxy::getObject(const char* objectName) const
{
  return fList.FindObject(objectName);
}

//_____________________________________________________________________________
TH1* MergeableCollectionProxy::histo(const char* objectName) const
{
  if (strchr(objectName, ':')) {
    TString action;

    TObjArray* arr = TString(objectName).Tokenize(":");

    if (arr->GetLast() > 0) {
      action = static_cast<TObjString*>(arr->At(1))->String();
      action.ToUpper();
    }

    TString oname = static_cast<TObjString*>(arr->At(0))->String();

    delete arr;

    TObject* o = getObject(oname);

    return fOC.histoWithAction("", o, action);
  }

  TObject* o = getObject(objectName);

  if (o && o->IsA()->InheritsFrom(TH1::Class())) {
    return static_cast<TH1*>(o);
  }

  return 0x0;
}

//_____________________________________________________________________________
TH2* MergeableCollectionProxy::h2(const char* objectName) const
{
  TObject* o = getObject(objectName);

  if (o->IsA()->InheritsFrom(TH2::Class())) {
    return static_cast<TH2*>(o);
  }
  return 0x0;
}

//_____________________________________________________________________________
TProfile* MergeableCollectionProxy::prof(const char* objectName) const
{
  TObject* o = getObject(objectName);

  if (o->IsA()->InheritsFrom(TProfile::Class())) {
    return static_cast<TProfile*>(o);
  }
  return 0x0;
}

//_____________________________________________________________________________
void MergeableCollectionProxy::Print(Option_t* opt) const
{
  fList.Print(opt);
}

//_____________________________________________________________________________
TIterator* MergeableCollectionProxy::createIterator(Bool_t dir) const
{
  return fList.MakeIterator(dir);
}
} // namespace o2::mch::eval
