//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) {
    rows = r;
    cols = c;
    linear = new T[r * c];
  }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() { delete[] linear; }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    data_ = new T *[r];
    for (int i = 0; i < r; i++) {
      data_[i] = this->linear + i * c;
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return this->rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return this->cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override { return data_[i][j]; }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override { data_[i][j] = val; }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    int k = 0;
    for (int i = 0; i < this->rows; i++) {
      for (int j = 0; j < this->cols; j++) {
        data_[i][j] = arr[k];
        k++;
      }
    }
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override { delete[] data_; }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    if (mat1->GetRows() != mat2->GetRows() || mat1->GetColumns() != mat2->GetColumns()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    std::unique_ptr<RowMatrix<T>> result_ptr{new RowMatrix<T>(mat1->GetRows(), mat1->GetColumns())};
    T linearArray[mat1->GetRows() * mat1->GetColumns()];
    int k = 0;
    for (int i = 0; i < mat1->GetRows(); i++) {
      for (int j = 0; j < mat1->GetColumns(); j++) {
        linearArray[k] = mat1->GetElem(i, j) + mat2->GetElem(i, j);
        k++;
      }
    }
    result_ptr->MatImport(&linearArray[0]);
    return result_ptr;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    if (mat1->GetColumns() != mat2->GetRows()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    std::unique_ptr<RowMatrix<T>> result_ptr{new RowMatrix<T>(mat1->GetRows(), mat2->GetColumns())};
    T linearArray[mat1->GetRows() * mat2->GetColumns()];
    for (int i = 0; i < mat1->GetRows() * mat2->GetColumns(); i++) {
      linearArray[i] = 0;
    }
    int l = 0;
    for (int i = 0; i < mat1->GetRows(); i++) {
      for (int j = 0; j < mat2->GetColumns(); j++) {
        for (int k = 0; k < mat2->GetRows(); k++) {
          linearArray[l] = linearArray[l] + mat1->GetElem(i, k) * mat2->GetElem(k, j);
        }
        l++;
      }
    }
    result_ptr->MatImport(&linearArray[0]);
    return result_ptr;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    // TODO(P0): Add code
    /*if (matA->GetRows() != matB->GetRows() || matA->GetColumns() != matB->GetColumns()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    std::unique_ptr<RowMatrix<T>> result_ptr1{new RowMatrix<T>(matA->GetRows(), matA->GetColumns())};
    T linearArray[matA->GetRows() * matA->GetColumns()];
    int k = 0;
    for (int i = 0; i < matA->GetRows(); i++) {
      for (int j = 0; j < matA->GetColumns(); j++) {
        linearArray[k] = matA->GetElem(i, j) + matB->GetElem(i, j);
        k++;
      }
    }
    result_ptr1->MatImport(&linearArray[0]);

    if (result_ptr1->GetColumns() != matC->GetRows()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    std::unique_ptr<RowMatrix<T>> result_ptr2{new RowMatrix<T>(result_ptr1->GetRows(), matC->GetColumns())};
    T linearArray2[result_ptr1->GetRows() * matC->GetColumns()];
    for (int i = 0; i < result_ptr1->GetRows() * matC->GetColumns(); i++) {
      linearArray2[i] = 0;
    }
    int l = 0;
    for (int i = 0; i < result_ptr1->GetRows(); i++) {
      for (int j = 0; j < matC->GetColumns(); j++) {
        for (int k = 0; k < matC->GetRows(); k++) {
          linearArray2[l] = linearArray2[l] + result_ptr1->GetElem(i, k) * matC->GetElem(k, j);
        }
        l++;
      }
    }
    result_ptr2->MatImport(&linearArray2[0]);
    return result_ptr2;*/

    std::unique_ptr<RowMatrix<T>> result_ptr1{new RowMatrix<T>(matA->GetRows(), matB->GetColumns())};
    result_ptr1 = MultiplyMatrices(std::move(matA), std::move(matB));
    if (result_ptr1 == nullptr) {
      return result_ptr1;
    }

    return AddMatrices(std::move(result_ptr1), std::move(matC));
  }
};
}  // namespace bustub
