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
    data_ = new T *[Matrix<T>::rows];
    for (int i = 0, l = 0; i < r; i++, l = l + c) {
      data_[i] = &(this->linear[l]);
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return Matrix<T>::rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return Matrix<T>::cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override {
    int linearPosition = i * this->GetColumns() + j;
    return (Matrix<T>::linear[linearPosition]);
  }

  void SetElem(int i, int j, T val) override {
    int linearPosition = i * this->GetColumns() + j;
    Matrix<T>::linear[linearPosition] = val;
  }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    int allocatedSpace = this->rows * this->cols * sizeof(T);
    memcpy(this->linear, arr, allocatedSpace);
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override { delete[] data_; };

 private:
  // 2D array containing the elements of the matrix in row-major format
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    std::unique_ptr<RowMatrix<T>> result_ptr{new RowMatrix<int>(mat1->GetRows(), mat1->GetColumns())};
    for (int i = 0; i < mat1->GetRows(); i++) {
      for (int j = 0; j < mat1->GetColumns(); j++) {
        int currSum = mat1->GetElem(i, j) + mat2->GetElem(i, j);
        result_ptr->SetElem(i, j, currSum);
      }
    }
    return result_ptr;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    std::unique_ptr<RowMatrix<T>> result_ptr{new RowMatrix<int>(mat1->GetRows(), mat2->GetColumns())};
    for (int i = 0; i < mat1->GetRows(); i++) {
      for (int j = 0; j < mat2->GetColumns(); j++) {
        int dotProduct = 0;
        for (int k = 0; k < mat1->GetColumns(); k++) {
          dotProduct += mat1->GetElem(i, k) * mat2->GetElem(k, j);
        }
        result_ptr->SetElem(i, j, dotProduct);
      }
    }
    return result_ptr;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    if (matC->GetRows() != matA->GetRows() || matC->GetColumns() != matB->GetColumns() ||
        matA->GetColumns() != matB->GetRows() || matA->GetRows() != matB->GetColumns()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    std::unique_ptr<RowMatrix<T>> multiplied_ptr{new RowMatrix<int>(matA->GetRows(), matB->GetColumns())};
    for (int i = 0; i < matA->GetRows(); i++) {
      for (int j = 0; j < matB->GetColumns(); j++) {
        int dotProduct = 0;
        for (int k = 0; k < matA->GetColumns(); k++) {
          dotProduct += matA->GetElem(i, k) * matB->GetElem(k, j);
        }
        multiplied_ptr->SetElem(i, j, dotProduct);
      }
    }

    std::unique_ptr<RowMatrix<T>> result_ptr{new RowMatrix<int>(matC->GetRows(), matC->GetColumns())};
    for (int i = 0; i < matC->GetRows(); i++) {
      for (int j = 0; j < matC->GetColumns(); j++) {
        int currSum = multiplied_ptr->GetElem(i, j) + matC->GetElem(i, j);
        result_ptr->SetElem(i, j, currSum);
      }
    }

    return result_ptr;
  }
};
}  // namespace bustub
